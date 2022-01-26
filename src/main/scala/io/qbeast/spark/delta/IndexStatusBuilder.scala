/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap

/**
 * Builds the index status from a given snapshot and revision
 * @param qbeastSnapshot the QbeastSnapshot
 * @param revision the revision
 * @param announcedSet the announced set available for the revision
 * @param replicatedSet the replicated set available for the revision
 */
private[delta] class IndexStatusBuilder(
    qbeastSnapshot: DeltaQbeastSnapshot,
    revision: Revision,
    replicatedSet: ReplicatedSet,
    announcedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  /**
   * Dataset of files belonging to the specific revision
   * @return the dataset of AddFile actions
   */
  def revisionFiles: Dataset[QbeastBlock] =
    // this must be external to the lambda, to avoid SerializationErrors
    qbeastSnapshot.loadRevisionBlocks(revision.revisionID)

  def build(): IndexStatus = {
    IndexStatus(
      revision = revision,
      replicatedSet = replicatedSet,
      announcedSet = announcedSet,
      cubesStatuses = buildCubesStatuses)
  }

  /**
   * Returns the index state for the given space revision
   * @return Dataset containing cube information
   */
  def buildCubesStatuses: SortedMap[CubeId, CubeStatus] = {

    val rev = revision
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]
    revisionFiles
      .collect()
      .toVector
      .groupBy(_.cube)
      .map { case (cube, files) =>
        var minMaxWeight = Int.MaxValue
        var elementCount = 0L
        for (file <- files) {
          elementCount += file.elementCount
          val maxWeight = file.maxWeight.value
          if (maxWeight < minMaxWeight) {
            minMaxWeight = maxWeight
          }
        }
        val cubeStatus = if (minMaxWeight == Int.MaxValue) {
          CubeStatus(Weight.MaxValue, NormalizedWeight(rev.desiredCubeSize, elementCount), files)
        } else {
          val w = Weight(minMaxWeight)
          CubeStatus(w, NormalizedWeight(w), files)
        }
        builder += ((rev.createCubeId(cube), cubeStatus))
      }
    builder.result()
  }

}
