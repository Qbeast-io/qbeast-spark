package io.qbeast.core.model

import io.qbeast.core.model.CubeWeightsBuilder.estimateGroupCubeSize
import io.qbeast.core.model.Weight.MaxValue

import scala.collection.mutable

object CubeWeightsBuilder {
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
  private[CubeWeightsBuilder] def estimateGroupCubeSize(
      desiredCubeSize: Int,
      numPartitions: Int,
      numElements: Long,
      bufferCapacity: Long): Int = {
    val numGroups = Math.max(numPartitions, numElements / bufferCapacity)
    val groupCubeSize = desiredCubeSize / numGroups
    Math.max(minGroupCubeSize, groupCubeSize.toInt)
  }

}

/**
 * Builder for creating cube weights.
 *
 * @param desiredCubeSize           the desired cube size
 * @param groupCubeSize                 the number of elements for each group
 * @param bufferCapacity the buffer capacity to store the cube weights in memory
 * @param replicatedOrAnnouncedSet the announced or replicated cubes' identifiers
 */
class CubeWeightsBuilder protected (
    private val desiredCubeSize: Int,
    private val groupCubeSize: Int,
    private val bufferCapacity: Long,
    private val replicatedOrAnnouncedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  def this(
      indexStatus: IndexStatus,
      numPartitions: Int,
      numElements: Long,
      bufferCapacity: Long) =
    this(
      indexStatus.revision.desiredCubeSize,
      estimateGroupCubeSize(
        indexStatus.revision.desiredCubeSize,
        numPartitions,
        numElements,
        bufferCapacity),
      bufferCapacity,
      indexStatus.replicatedOrAnnouncedSet)

  private class WeightAndTreeSize(var weight: NormalizedWeight, var treeSize: Double)

  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  protected val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)
  private var resultBuffer = Seq.empty[CubeDomain]

  /**
   * Updates the builder with given point with weight.
   *
   * @param point  the point
   * @param weight the weight
   * @param parent the parent cube identifier used to find
   *               the container cube if available
   * @return this instance
   */
  def update(point: Point, weight: Weight, parent: Option[CubeId] = None): CubeWeightsBuilder = {
    queue.enqueue(PointWeightAndParent(point, weight, parent))
    if (queue.size >= bufferCapacity) {
      resultBuffer = resultBuffer ++ resultInternal()
    }
    this
  }

  /**
   * Builds the resulting cube weights sequence.
   *
   * @return the resulting cube weights map
   */
  def result(): Seq[CubeDomain] = {
    resultBuffer ++ resultInternal()
  }

  def resultInternal(): Seq[CubeDomain] = {
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
        val weightAndCount = weights.getOrElseUpdate(cubeId, new WeightAndCount(MaxValue, 0))
        if (weightAndCount.count < groupCubeSize) {
          weightAndCount.count += 1
          if (weightAndCount.count == groupCubeSize) {
            weightAndCount.weight = weight
          }
          continue = replicatedOrAnnouncedSet.contains(cubeId)
        }
      }
    }

    // Convert the information into a Map[CubeId, (NormalizedWeight, Double)]
    // from which the cube domains are computed.
    val weightsAndCubeSizes = weights.map { case (cubeId, weightAndCount) =>
      val nw =
        if (weightAndCount.count == groupCubeSize) NormalizedWeight(weightAndCount.weight)
        else NormalizedWeight(desiredCubeSize, weightAndCount.count)
      cubeId -> new WeightAndTreeSize(nw, weightAndCount.count)
    }.toMap

    if (weightsAndCubeSizes.nonEmpty) computeCubeDomains(weightsAndCubeSizes)
    else Seq.empty[CubeDomain]
  }

  /**
   * Compute cube domain from an unpopulated tree, which has cube NormalizedWeight and cube size.
   * Computation is done from bottom-up, at each step we update the parent tree size with that
   * of the current cube, and compute cube domain using treeSize / (1d - parentWeight).
   * @param weightsAndTreeSizes Map containing cube weight and cube size. Cube size is constantly updated
   *                            to cube tree size before it's used to compute cube domain
   * @return Map of cube bytes and domain
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
            val cTreeSize = weightsAndTreeSizes(cube).treeSize
            val pInfo = weightsAndTreeSizes(parent)

            // Compute cube domain
            val domain = cTreeSize / (1d - pInfo.weight)
            cubeDomainBuilder += CubeDomain(cube.bytes, domain)

            // Update parent treeSize
            pInfo.treeSize += cTreeSize
          case None =>
        }
      })
    }

    // Top level cube domain = treeSize
    levelCubes(minLevel).foreach { cube =>
      val ts = weightsAndTreeSizes(cube).treeSize
      cubeDomainBuilder += CubeDomain(cube.bytes, ts)
    }

    cubeDomainBuilder.result()
  }

}

/**
 * Weight and count.
 *
 * @param weight the weight
 * @param count  the count
 */
private class WeightAndCount(var weight: Weight, var count: Int)

/**
 * Point, weight and parent cube identifier if available.
 *
 * @param point  the point
 * @param weight the weight
 * @param parent the parent
 */
protected case class PointWeightAndParent(point: Point, weight: Weight, parent: Option[CubeId])

/**
 * Cube bytes and its domain size from a given data partition. A cube's domain size
 * is defined as the number of records in a partitions that has values that fall within
 * a cube's space.
 * @param cubeBytes Array[Byte] unique to a cube
 * @param domain The number of records in a partition that fit in the said cube
 */
case class CubeDomain(cubeBytes: Array[Byte], domain: Double)
