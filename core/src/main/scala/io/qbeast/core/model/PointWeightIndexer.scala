package io.qbeast.core.model

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
   * @param changes the table changes
   * @return the PointWeightIndexer
   */
  def apply(changes: TableChanges): PointWeightIndexer =
    new PointWeightIndexer(changes.cubeWeights, changes.announcedOrReplicatedSet)

}
