/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.index.Weight.MaxValue
import io.qbeast.spark.model.Point

import scala.collection.mutable

/**
 * Utility object which helps working with the cube weights.
 */
object CubeWeights extends Serializable {

  /**
   * Finds the target cube identifiers for given point with the specified weight.
   *
   * @param point the point
   * @param weight the point weight
   * @param cubeWeights the cube weights
   * @param announcedSet the announced cube identifiers
   * @param replicatedSet the replicated cube identifiers
   * @param parentCubeId the parent cube identifier to find the
   *                     cubes containing the point (exclusive).
   * @return the target cube identifiers
   */
  def findTargetCubeIds(
      point: Point,
      weight: Weight,
      cubeWeights: Map[CubeId, Weight],
      announcedSet: Set[CubeId],
      replicatedSet: Set[CubeId],
      parentCubeId: Option[CubeId] = None): Seq[CubeId] = {
    val builder = Seq.newBuilder[CubeId]
    val containers = parentCubeId match {
      case Some(parent) => CubeId.containers(point, parent)
      case None => CubeId.containers(point)
    }
    var continue = true
    while (continue && containers.hasNext) {
      val cubeId = containers.next()
      cubeWeights.get(cubeId) match {
        case Some(cubeWeight) if weight <= cubeWeight =>
          builder += cubeId
          continue = announcedSet.contains(cubeId) || replicatedSet.contains(cubeId)
        case None =>
          builder += cubeId
          continue = false
        case _ =>
          ()
      }
    }
    builder.result()
  }

  /**
   * Merges given cube weights with estimated cube weights.
   *
   * @param cubeNormalizedWeight the cube weights
   * @param estimatedCubeNormalizedWeights the estimated cube weights
   * @return the merged cube weights
   */
  def merge(
      cubeNormalizedWeight: Map[CubeId, NormalizedWeight],
      estimatedCubeNormalizedWeights: Map[CubeId, NormalizedWeight]): Map[CubeId, Weight] = {
    {
      if (cubeNormalizedWeight.isEmpty) {
        estimatedCubeNormalizedWeights
      } else if (estimatedCubeNormalizedWeights.isEmpty) {
        cubeNormalizedWeight
      } else {
        val builder = Map.newBuilder[CubeId, NormalizedWeight]
        for ((cubeId, weight) <- cubeNormalizedWeight) {
          val mergedWeight = estimatedCubeNormalizedWeights.get(cubeId) match {
            case Some(estimatedWeight) => NormalizedWeight.merge(weight, estimatedWeight)
            case None => weight
          }
          builder += (cubeId -> mergedWeight)
        }
        for ((cubeId, estimatedWeight) <- estimatedCubeNormalizedWeights) {
          if (!cubeNormalizedWeight.contains(cubeId)) {
            builder += (cubeId -> estimatedWeight)
          }
        }
        builder.result()
      }
    }.mapValues(NormalizedWeight.toWeight)
      .map(identity)
    // We need this because mapValues is not serializable https://github.com/scala/bug/issues/7005

  }

}

/**
 * Builder for creating cube weights.
 *
 * @param desiredSize the desired cube size
 * @param announcedSet the announced cube identifiers
 * @param replicatedSet the replicated cube identifiers
 */
class CubeWeightsBuilder(
    private val desiredSize: Int,
    private val numPartitions: Int,
    announcedSet: Set[CubeId] = Set.empty,
    replicatedSet: Set[CubeId] = Set.empty) {
  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  private val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)

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
        if (weightAndCount.count < desiredSize) {
          weightAndCount.count += 1
          if (weightAndCount.count == desiredSize) {
            weightAndCount.weight = weight
          }
          continue = announcedSet.contains(cubeId) || replicatedSet.contains(cubeId)
        }
      }
    }
    weights.map {
      case (cubeId, weightAndCount) if weightAndCount.count == desiredSize =>
        CubeNormalizedWeight(
          cubeId.bytes,
          NormalizedWeight(weightAndCount.weight) * numPartitions)
      case (cubeId, weightAndCount) =>
        CubeNormalizedWeight(
          cubeId.bytes,
          NormalizedWeight(desiredSize, weightAndCount.count) * numPartitions)
    }.toSeq
  }

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
private case class PointWeightAndParent(point: Point, weight: Weight, parent: Option[CubeId])

/**
 * Cube and NormalizedWeight
 *
 * @param cubeBytes the cube
 * @param normalizedWeight the weight
 */
case class CubeNormalizedWeight(cubeBytes: Array[Byte], normalizedWeight: NormalizedWeight)
