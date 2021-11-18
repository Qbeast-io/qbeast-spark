package io.qbeast.model

/**
 * Indexes a point by its weight and cube transformation.
 * @param cubeWeights the map of cube and weight
 * @param announcedOrReplicatedSet the set of cubes in announced or replicated state
 */
class PointWeightIndexer(
    val cubeWeights: Map[CubeId, Weight],
    val announcedOrReplicatedSet: Set[CubeId])
    extends Serializable {

  /**
   * Finds the target cube identifiers for given point with the specified weight.
   *
   * @param point the point
   * @param weight the point weight
   * @param parentCubeId the parent cube identifier to find the
   *                     cubes containing the point (exclusive).
   * @return the target cube identifiers
   */
  def findTargetCubeIds(
      point: Point,
      weight: Weight,
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
          continue = announcedOrReplicatedSet.contains(cubeId)
        case None =>
          builder += cubeId
          continue = false
        case _ =>
          ()
      }
    }
    builder.result()
  }

}

object PointWeightIndexer {

  /**
   * Builds a new point weight indexer from the status changes
   * @param isc the index status changes
   * @return
   */
  def buildNewWeightIndexer(isc: IndexStatusChange): PointWeightIndexer =
    buildNewWeightIndexer(
      isc.supersededIndexStatus.cubeNormalizedWeights,
      isc.deltaNormalizedCubeWeights,
      isc.announcedOrReplicatedSet)

  /**
   * Builds a new weight indexer.
   * @param previousStateNormalizedWeights the previous state of the NormalizedCubeWeights
   * @param deltaNormalizedCubeWeights the newly computed NormalizedCubeWeights
   * @param announcedOrReplicatedSet the set of cubes in announced or replicated state
   * @return
   */
  def buildNewWeightIndexer(
      previousStateNormalizedWeights: Map[CubeId, NormalizedWeight],
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
      announcedOrReplicatedSet: Set[CubeId]): PointWeightIndexer = {
    val mergedWeights =
      mergeNormalizedWeights(previousStateNormalizedWeights, deltaNormalizedCubeWeights)
    new PointWeightIndexer(
      cubeWeights = mergedWeights,
      announcedOrReplicatedSet = announcedOrReplicatedSet)

  }

  /**
   * This method return the merge between the previous state of the index status
   * and the newly computed NormalizedCubeWeights.
   * @param previousStateNormalizedWeights the previous state of the NormalizedCubeWeights
   * @param deltaNormalizedCubeWeights the newly computed NormalizedCubeWeights
   * @return
   */
  def mergeNormalizedWeights(
      previousStateNormalizedWeights: Map[CubeId, NormalizedWeight],
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight]): Map[CubeId, Weight] = {
    {
      if (previousStateNormalizedWeights.isEmpty) {
        deltaNormalizedCubeWeights
      } else if (deltaNormalizedCubeWeights.isEmpty) {
        previousStateNormalizedWeights
      } else {
        val builder = Map.newBuilder[CubeId, NormalizedWeight]
        for ((cubeId, weight) <- previousStateNormalizedWeights) {
          val mergedWeight = deltaNormalizedCubeWeights.get(cubeId) match {
            case Some(estimatedWeight) => NormalizedWeight.merge(weight, estimatedWeight)
            case None => weight
          }
          builder += (cubeId -> mergedWeight)
        }
        for ((cubeId, estimatedWeight) <- deltaNormalizedCubeWeights) {
          if (!previousStateNormalizedWeights.contains(cubeId)) {
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
