package io.qbeast.core.model

import scala.collection.mutable

object CubeDomainsBuilder {
  val minGroupCubeSize = 30

  /**
   * Estimates the groupCubeSize depending on the input parameters.
   * The formula to compute the estimated value is the following:
   *   numGroups = MAX(numPartitions, (numElements / cubeWeightsBufferCapacity))
   *   groupCubeSize = desiredCubeSize / numGroups
   * @param desiredCubeSize the desired cube size
   * @param numPartitions the number of partitions
   * @param numElements the total number of elements in the input data
   * @param bufferCapacity buffer capacity; number of elements that fit in memory
   * @return the estimated groupCubeSize as a Double.
   */
  private def estimateGroupCubeSize(
      desiredCubeSize: Int,
      numPartitions: Int,
      numElements: Long,
      bufferCapacity: Long): Int = {
    val numGroups = Math.max(numPartitions, numElements / bufferCapacity)
    val groupCubeSize = desiredCubeSize / numGroups
    Math.max(minGroupCubeSize, groupCubeSize.toInt)
  }

  def apply(
      indexStatus: IndexStatus,
      numPartitions: Int,
      numElements: Long,
      bufferCapacity: Long): CubeDomainsBuilder = {
    val desiredCubeSize = indexStatus.revision.desiredCubeSize
    val groupCubeSize = estimateGroupCubeSize(
      indexStatus.revision.desiredCubeSize,
      numPartitions,
      numElements,
      bufferCapacity)

    new CubeDomainsBuilder(
      desiredCubeSize,
      groupCubeSize,
      bufferCapacity,
      indexStatus.replicatedOrAnnouncedSet,
      indexStatus.cubesStatuses)
  }

}

/**
 * Builder for creating cube domains.
 * @param groupCubeSize  the number of elements for each group
 * @param bufferCapacity the buffer capacity to store the cube weights in memory
 * @param replicatedOrAnnouncedSet the announced or replicated cubes' identifiers
 * @param existingNormalizedWeights the cube NormalizedWeights for the existing index
 */
class CubeDomainsBuilder protected (
    private val desiredCubeSize: Int,
    private val groupCubeSize: Int,
    private val bufferCapacity: Long,
    private val replicatedOrAnnouncedSet: Set[CubeId] = Set.empty,
    private val existingCubeStatuses: Map[CubeId, CubeStatus] = Map.empty)
    extends Serializable {

  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  private val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)
  private var resultBuffer = Seq.empty[CubeDomain]

  private val weightAndCountFactory =
    WeightAndCountFactory(desiredCubeSize, groupCubeSize, existingCubeStatuses)

  /**
   * Updates the builder with given point with weight.
   *
   * @param point  the point
   * @param weight the weight
   * @param parent the parent cube identifier used to find
   *               the container cube if available
   * @return this instance
   */
  def update(point: Point, weight: Weight, parent: Option[CubeId] = None): CubeDomainsBuilder = {
    queue.enqueue(PointWeightAndParent(point, weight, parent))
    if (queue.size >= bufferCapacity) {
      resultBuffer ++= resultInternal()
    }
    this
  }

  /**
   * Builds the resulting cube domain sequence.
   *
   * @return the resulting cube domain map
   */
  def result(): Seq[CubeDomain] = {
    resultInternal() ++ resultBuffer
  }

  def resultInternal(): Seq[CubeDomain] = {
    val weightAndSize = computeWeightsAndSizes()
    if (weightAndSize.nonEmpty) computeCubeDomains(weightAndSize)
    else Seq.empty[CubeDomain]
  }

  /**
   * Compute OTree cube weights and sizes.
   */
  private def computeWeightsAndSizes(): Map[CubeId, WeightAndTreeSize] = {
    val weights = mutable.Map.empty[CubeId, WeightAndCount]
    while (queue.nonEmpty) {
      val PointWeightAndParent(point, weight, parent) = queue.dequeue()
      val containers = parent match {
        case Some(parentCubeId) => CubeId.containers(point, parentCubeId)
        case None => CubeId.containers(point)
      }
      var continue = true
      while (continue && containers.hasNext) {
        val cubeId = containers.next()
        val weightAndCount =
          weights.getOrElseUpdate(cubeId, weightAndCountFactory.createWeightAndCount(cubeId))
        if (weightAndCount.shouldInclude(weight, groupCubeSize)) {
          weightAndCount.update(weight)
          continue = replicatedOrAnnouncedSet.contains(cubeId)
        }
      }
    }

    // Convert cube Weight and size into a Map[CubeId, (NormalizedWeight, Double)] from which
    // the cube domains are computed.
    weights.map { case (cubeId, weightAndCount) =>
      cubeId -> weightAndCount.toWeightAndTreeSize(groupCubeSize)
    }.toMap

  }

  /**
   * Compute cube domain from an unpopulated tree, which has cube NormalizedWeight and cube size.
   * Done in a bottom-up, updating at each step the parent tree size with that of the current cube,
   * and compute cube domain using treeSize / (1d - parentWeight).
   * @param weightsAndTreeSizes NormalizedWeight and size for each cube. Cube size is constantly
   *                            updated to reach tree size before it's used to compute cube domain
   * @return Sequence of cube bytes and domain
   */
  private def computeCubeDomains(
      weightsAndTreeSizes: Map[CubeId, WeightAndTreeSize]): Seq[CubeDomain] = {
    val cubeDomainBuilder = Seq.newBuilder[CubeDomain]
    cubeDomainBuilder.sizeHint(weightsAndTreeSizes.size)

    // Compute cube domain from bottom-up
    val levelCubes = weightsAndTreeSizes.keys.groupBy(_.depth)
    val minLevel = levelCubes.keys.min
    val maxLevel = levelCubes.keys.max

    (maxLevel until minLevel by -1) foreach { level =>
      levelCubes(level).foreach(cube => {
        cube.parent match {
          case Some(parent) =>
            val cubeTreeSize = weightsAndTreeSizes(cube).treeSize
            val parentInfo = weightsAndTreeSizes(parent)

            // Compute cube domain
            val domain = cubeTreeSize / (1d - parentInfo.weight)
            cubeDomainBuilder += CubeDomain(cube.bytes, domain)

            // Update parent treeSize
            parentInfo.treeSize += cubeTreeSize
          case None =>
        }
      })
    }

    // Top level cube domain = treeSize
    levelCubes(minLevel).foreach { cube =>
      val domain = weightsAndTreeSizes(cube).treeSize
      cubeDomainBuilder += CubeDomain(cube.bytes, domain)
    }

    cubeDomainBuilder.result()
  }

}

/**
 * Cube bytes and its domain size from a given data partition, with domain defined as
 * the number of records in a partition that has values within a cube's space limits.
 * @param cubeBytes Array[Byte] unique to a cube
 * @param domain The number of records in a partition that fit in the said cube
 */
final case class CubeDomain(cubeBytes: Array[Byte], domain: Double)
