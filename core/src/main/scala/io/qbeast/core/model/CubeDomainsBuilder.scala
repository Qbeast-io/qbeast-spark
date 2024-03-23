package io.qbeast.core.model

import io.qbeast.core.model.Weight.MaxValue

import scala.collection.mutable

object CubeDomainsBuilder {
  val minGroupCubeSize = 30

  /**
   * Estimates the groupCubeSize depending on the input parameters. The formula to compute the
   * estimated value is the following: numGroups = MAX(numPartitions, (numElements /
   * cubeWeightsBufferCapacity)) groupCubeSize = desiredCubeSize / numGroups
   * @param desiredCubeSize
   *   the desired cube size
   * @param numPartitions
   *   the number of partitions
   * @param numElements
   *   the total number of elements in the input data
   * @param bufferCapacity
   *   buffer capacity; number of elements that fit in memory
   * @return
   *   the estimated groupCubeSize as a Double.
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

  /**
   * Compute cube NormalizedWeights to properly reflect leaf cube elementCounts.
   */
  private def computeNormalizedWeights(
      desiredCubeSize: Int,
      groupCubeSize: Int,
      cubeStatuses: Map[CubeId, CubeStatus]): Map[CubeId, NormalizedWeight] = {
    val isLeaf = (cubeId: CubeId) => !cubeId.children.exists(cubeStatuses.contains)
    val multiplier = groupCubeSize / desiredCubeSize.toDouble

    cubeStatuses.map {
      case (cubeId, status) if isLeaf(cubeId) =>
        val cubeSize = status.blocks.map(_.elementCount).sum
        val groupSize = (cubeSize * multiplier).toLong.max(1L)
        val nw = NormalizedWeight(groupCubeSize, groupSize).max(1.0)
        cubeId -> nw
      case (cubeId, status) => cubeId -> status.normalizedWeight
    }
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
    val factory = new WeightAndCountFactory(
      computeNormalizedWeights(desiredCubeSize, groupCubeSize, indexStatus.cubesStatuses),
      groupCubeSize)

    new CubeDomainsBuilder(
      desiredCubeSize,
      groupCubeSize,
      bufferCapacity,
      factory,
      indexStatus.replicatedOrAnnouncedSet)
  }

}

/**
 * Builder for creating cube domains.
 * @param groupCubeSize
 *   the number of elements for each group
 * @param bufferCapacity
 *   the buffer capacity to store the cube weights in memory
 * @param replicatedOrAnnouncedSet
 *   the announced or replicated cubes' identifiers
 * @param existingNormalizedWeights
 *   the cube NormalizedWeights for the existing index
 */
class CubeDomainsBuilder protected (
    private val desiredCubeSize: Int,
    private val groupCubeSize: Int,
    private val bufferCapacity: Long,
    private val weightAndCountFactory: WeightAndCountFactory,
    private val replicatedOrAnnouncedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  private val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)
  private var resultBuffer = Seq.empty[CubeDomain]

  /**
   * Updates the builder with given point with weight.
   *
   * @param point
   *   the point
   * @param weight
   *   the weight
   * @param parent
   *   the parent cube identifier used to find the container cube if available
   * @return
   *   this instance
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
   * @return
   *   the resulting cube domain map
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
          weights.getOrElseUpdate(cubeId, weightAndCountFactory.create(cubeId))
        if (weightAndCount.shouldInclude(weight, groupCubeSize)) {
          weightAndCount.update(weight)
          continue = replicatedOrAnnouncedSet.contains(cubeId)
        }
      }
    }

    // Convert cube Weight and size into a Map[CubeId, (NormalizedWeight, Double)] from which
    // the cube domains are computed.
    weights.map { case (cubeId, weightAndCount) =>
      cubeId -> weightAndCount.toWeightAndTreeSize(groupCubeSize, desiredCubeSize)
    }.toMap

  }

  /**
   * Compute cube domain from an unpopulated tree, which has cube NormalizedWeight and cube size.
   * Done in a bottom-up, updating at each step the parent tree size with that of the current
   * cube, and compute cube domain using treeSize / (1d - parentWeight).
   * @param weightsAndTreeSizes
   *   NormalizedWeight and size for each cube. Cube size is constantly updated to reach tree size
   *   before it's used to compute cube domain
   * @return
   *   Sequence of cube bytes and domain
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
 * Factory for WeightAndCount. The creation of which depends on whether the associated CubeId is
 * present in the existing index, if it's a inner or leaf cube.
 *
 * @param existingNormalizedWeights
 *   Cube Weight of the existing index
 * @param groupCubeSize
 *   partition-level cube size for indexing
 */
private class WeightAndCountFactory(
    existingNormalizedWeights: Map[CubeId, NormalizedWeight],
    groupCubeSize: Int) {

  def create(cubeId: CubeId): WeightAndCount =
    existingNormalizedWeights.get(cubeId) match {
      case Some(nw: NormalizedWeight) if nw < 1.0 =>
        new InnerCubeWeightAndCount(Weight(nw))
      case Some(nw: NormalizedWeight) =>
        val existingLeafSize = ((groupCubeSize - 1) / nw).toInt
        new LeafCubeWeightAndCount(existingLeafSize)
      case None => new WeightAndCount(MaxValue, 0)
    }

}

/**
 * Weight and count for a CubeId.
 * @param weight
 *   Weight of the associated CubeId
 * @param count
 *   Element count of the associated CubeId
 */
private class WeightAndCount(var weight: Weight, var count: Int) {

  /**
   * Determines whether an instance, with its instanceWeight, should be included in the associated
   * cube
   * @param instanceWeight
   *   Weight of a given instance
   * @param limit
   *   total capacity of the cube i.e. groupCubeSize
   */
  def shouldInclude(instanceWeight: Weight, limit: Int): Boolean =
    count < limit

  /**
   * Update metadata to include the instance
   * @param instanceWeight
   *   Weight of a given instance
   */
  def update(instanceWeight: Weight): Unit = {
    count += 1
    weight = instanceWeight
  }

  /**
   * Convert to WeightAndTreeSize, with cube size i.e. count as the initial tree size value.
   * @param gcs
   *   groupCubeSize
   * @param dcs
   *   desiredCubeSize
   */
  def toWeightAndTreeSize(gcs: Int, dcs: Int): WeightAndTreeSize = {
    val nw = toNormalizedWeight(gcs, dcs)
    new WeightAndTreeSize(nw, cubeSize)
  }

  /**
   * @return
   *   the number of added instances to the associated cube
   */
  def cubeSize: Int = count

  /**
   * Compute NormalizedWeight according to the state of the cube
   * @param gcs
   *   groupCubeSize
   * @param dcs
   *   desiredCubeSize
   */
  def toNormalizedWeight(gcs: Int, dcs: Int): NormalizedWeight =
    if (count == gcs) NormalizedWeight(weight)
    else NormalizedWeight(dcs, cubeSize)

}

/**
 * WeightAndCount for an existing leaf cube. It can accept up to (groupCubeSize - start) records.
 */
private class LeafCubeWeightAndCount(start: Int) extends WeightAndCount(MaxValue, start) {

  override def cubeSize: Int = count - start

}

/**
 * WeightAndCount for an existing inner cube. It can accept up to groupCubeSize records, with the
 * additional constraint of instanceWeight < existingWeight.
 * @param existingWeight
 *   Weight for the associated existing CubeId
 */
private class InnerCubeWeightAndCount(existingWeight: Weight)
    extends WeightAndCount(existingWeight, 0) {

  override def shouldInclude(instanceWeight: Weight, limit: Int): Boolean = {
    instanceWeight < existingWeight && count < limit
  }

  override def toNormalizedWeight(gcs: Int, dcs: Int): NormalizedWeight = {
    // An existing inner cube should always remain as such
    NormalizedWeight(weight)
  }

}

/**
 * Point, weight and parent cube identifier if available.
 *
 * @param point
 *   the point
 * @param weight
 *   the weight
 * @param parent
 *   the parent
 */
private case class PointWeightAndParent(point: Point, weight: Weight, parent: Option[CubeId])

/**
 * NormalizedWeight and tree size of a given cube, with tree size defined as the sum of its own
 * payload and all that of its descendents. The tree size is initialized with cube size and
 * updated to the tree size during computation before being used.
 * @param weight
 *   NormalizedWeight
 * @param treeSize
 *   Cube tree size
 */
private class WeightAndTreeSize(val weight: NormalizedWeight, var treeSize: Double)

/**
 * Cube bytes and its domain size from a given data partition, with domain defined as the number
 * of records in a partition that has values within a cube's space limits.
 * @param cubeBytes
 *   Array[Byte] unique to a cube
 * @param domain
 *   The number of records in a partition that fit in the said cube
 */
final case class CubeDomain(cubeBytes: Array[Byte], domain: Double)
