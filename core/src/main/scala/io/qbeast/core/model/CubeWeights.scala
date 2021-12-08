package io.qbeast.core.model

import io.qbeast.core.model.Weight.MaxValue

import scala.collection.mutable

/**
 * Builder for creating cube weights.
 *
 * @param desiredCubeSize the desired cube size
 * @param boostSize       the boost size
 * @param announcedSet the announced cube identifiers
 * @param replicatedSet the replicated cube identifiers
 */
class CubeWeightsBuilder(
    private val desiredCubeSize: Int,
    private val boostSize: Double,
    announcedSet: Set[CubeId] = Set.empty,
    replicatedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  def this(indexStatus: IndexStatus, boostSize: Double) =
    this(
      indexStatus.revision.desiredCubeSize,
      boostSize,
      indexStatus.announcedSet,
      indexStatus.replicatedSet)

  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  protected val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)

  /**
   * Updates the builder with given point with weight.
   *
   * @param point the point
   * @param weight the weight
   * @param parent the parent cube identifier used to find
   *               the container cube if available
   * @return this instance
   */
  def update(point: Point, weight: Weight, parent: Option[CubeId] = None): CubeWeightsBuilder = {
    queue.enqueue(PointWeightAndParent(point, weight, parent))
    this
  }

  /**
   * Builds the resulting cube weights sequence.
   *
   * @return the resulting cube weights map
   */
  def result(): Seq[CubeNormalizedWeight] = {
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
        if (weightAndCount.count < boostSize) {
          weightAndCount.count += 1
          if (weightAndCount.count >= desiredCubeSize) {
            weightAndCount.weight = weight
          } else if (weightAndCount.count == boostSize) {
            weightAndCount.weight = weight
          }
          continue = announcedSet.contains(cubeId) || replicatedSet.contains(cubeId)
        }
      }
    }
    weights.map {
      case (cubeId, weightAndCount) if weightAndCount.count > desiredCubeSize =>
        CubeNormalizedWeight(cubeId.bytes, NormalizedWeight(weightAndCount.weight))
      case (cubeId, weightAndCount) if weightAndCount.count == boostSize =>
        val s = desiredCubeSize / boostSize
        CubeNormalizedWeight(cubeId.bytes, NormalizedWeight(weightAndCount.weight) * s)
      case (cubeId, weightAndCount) =>
        CubeNormalizedWeight(
          cubeId.bytes,
          NormalizedWeight(desiredCubeSize, weightAndCount.count))
    }.toSeq
  }

}

/**
 * This class wraps the CubeWeightBuilder, but it limits the number of elements to keep in
 * memory to groupSize. There is a trade off between having a large maxGroupSize and thus estimating
 * a better index structure vs reducing the memory pressure.
 * @param indexStatus the current index status
 * @param boostSize   the boost size
 * @param maxGroupSize   the maximum number of elements to keep in memory
 */
class GroupedCubeWeightsBuilder(indexStatus: IndexStatus, boostSize: Double, maxGroupSize: Long)
    extends CubeWeightsBuilder(indexStatus, boostSize) {
  var resultBuffer = Seq.empty[CubeNormalizedWeight]
  var groupCount = 0

  /**
   * Updates the builder with given point with weight.
   *
   * @param point  the point
   * @param weight the weight
   * @param parent the parent cube identifier used to find
   *               the container cube if available
   * @return this instance
   */
  override def update(
      point: Point,
      weight: Weight,
      parent: Option[CubeId]): CubeWeightsBuilder = {
    super.update(point, weight, parent)
    groupCount += 1
    if (groupCount > maxGroupSize) {
      resultBuffer ++= super.result()
      groupCount = 0
      queue.clear()
    }
    this
  }

  /**
   * Builds the resulting cube weights sequence.
   *
   * @return the resulting cube weights map
   */
  override def result(): Seq[CubeNormalizedWeight] = resultBuffer ++ super.result()
}

/**
 * Weight and count.
 *
 * @param weight the weight
 * @param count the count
 */
private class WeightAndCount(var weight: Weight, var count: Int)

/**
 * Point, weight and parent cube identifier if available.
 *
 * @param point the point
 * @param weight the weight
 * @param parent the parent
 */
protected case class PointWeightAndParent(point: Point, weight: Weight, parent: Option[CubeId])

/**
 * Cube and NormalizedWeight
 *
 * @param cubeBytes the cube
 * @param normalizedWeight the weight
 */
case class CubeNormalizedWeight(cubeBytes: Array[Byte], normalizedWeight: NormalizedWeight)
