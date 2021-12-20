package io.qbeast.core.model

import io.qbeast.core.model.CubeWeightsBuilder.estimateGroupCubeSize
import io.qbeast.core.model.Weight.MaxValue

import scala.collection.mutable

object CubeWeightsBuilder {

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
      bufferCapacity: Long): Double = {
    val numGroups = Math.max(numPartitions, numElements / bufferCapacity)
    val groupCubeSize =
      Math.ceil(desiredCubeSize.toDouble / numGroups + 1)
    groupCubeSize
  }

}

/**
 * Builder for creating cube weights.
 *
 * @param desiredCubeSize           the desired cube size
 * @param groupSize                 the boost size
 * @param bufferCapacity the buffer capacity to store the cube weights in memory
 * @param announcedSet              the announced cube identifiers
 * @param replicatedSet             the replicated cube identifiers
 */
class CubeWeightsBuilder protected (
    private val desiredCubeSize: Int,
    private val groupSize: Double,
    private val bufferCapacity: Long,
    private val announcedSet: Set[CubeId],
    private val replicatedSet: Set[CubeId])
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
      indexStatus.announcedSet,
      indexStatus.replicatedSet)

  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  protected val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)
  private var resultBuffer = Seq.empty[CubeNormalizedWeight]

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
      resultBuffer ++= result()
      queue.clear()
    }
    this
  }

  /**
   * Builds the resulting cube weights sequence.
   *
   * @return the resulting cube weights map
   */
  def result(): Seq[CubeNormalizedWeight] = {
    resultInternal() ++ resultBuffer
  }

  def resultInternal(): Seq[CubeNormalizedWeight] = {
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
        if (weightAndCount.count < groupSize) {
          weightAndCount.count += 1
          if (weightAndCount.count >= desiredCubeSize || weightAndCount.count == groupSize) {
            weightAndCount.weight = weight
          }
          continue = announcedSet.contains(cubeId) || replicatedSet.contains(cubeId)
        }
      }
    }
    weights.map {
      case (cubeId, weightAndCount) if weightAndCount.count > desiredCubeSize =>
        CubeNormalizedWeight(cubeId.bytes, NormalizedWeight(weightAndCount.weight))
      case (cubeId, weightAndCount) if weightAndCount.count == groupSize =>
        val s = desiredCubeSize / groupSize
        CubeNormalizedWeight(cubeId.bytes, NormalizedWeight(weightAndCount.weight) * s)
      case (cubeId, weightAndCount) =>
        CubeNormalizedWeight(
          cubeId.bytes,
          NormalizedWeight(desiredCubeSize, weightAndCount.count))
    }.toSeq
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
 * Cube and NormalizedWeight
 *
 * @param cubeBytes        the cube
 * @param normalizedWeight the weight
 */
case class CubeNormalizedWeight(cubeBytes: Array[Byte], normalizedWeight: NormalizedWeight)
