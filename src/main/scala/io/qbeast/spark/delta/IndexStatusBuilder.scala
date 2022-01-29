/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.{Dataset, SparkSession}

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
  def revisionFiles: Dataset[(String, QbeastBlock)] = {
    // this must be external to the lambda, to avoid SerializationErrors
    val spark = SparkSession.active
    import spark.implicits._
    qbeastSnapshot
      .loadRevisionBlocks(revision.revisionID)
      .map(addFile =>
        (
          addFile.tags("cube"),
          QbeastBlock(addFile.path, addFile.tags, addFile.size, addFile.modificationTime)))
  }

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

    val spark = SparkSession.active
    import spark.implicits._
    val rev = revision
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]
    revisionFiles
      .groupByKey(_._1)
      .mapGroups((cube, f) => {
        var minMaxWeight = Int.MaxValue
        var elementCount = 0L
        val files = Vector.newBuilder[QbeastBlock]
        for ((_, file) <- f) {
          elementCount += file.elementCount
          val maxWeight = file.maxWeight.value
          if (maxWeight < minMaxWeight) {
            minMaxWeight = maxWeight
          }
          files += file
        }
        val cubeId = rev.createCubeId(cube)
        val cubeStatus = if (minMaxWeight == Int.MaxValue) {
          CubeStatus(
            cubeId,
            Weight.MaxValue,
            NormalizedWeight(rev.desiredCubeSize, elementCount),
            files.result())
        } else {
          val w = Weight(minMaxWeight)
          CubeStatus(cubeId, w, NormalizedWeight(w), files.result())
        }
        (cubeStatus.cubeId, cubeStatus)
      })
      .collect()
      .foreach(builder += _)
    builder.result()
  }

}
