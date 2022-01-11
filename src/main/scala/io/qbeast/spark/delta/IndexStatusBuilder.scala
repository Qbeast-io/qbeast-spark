/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.delta.actions.AddFile
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
  def revisionFiles: Dataset[AddFile] = {
    // this must be external to the lambda, to avoid SerializationErrors
    val revID = revision.revisionID.toString
    qbeastSnapshot.snapshot.allFiles.filter(_.tags(TagUtils.revision) == revID)
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
      .groupByKey(_.tags(TagUtils.cube))
      .mapGroups((cube, f) => {
        var minMaxWeight = Int.MaxValue
        var elementCount = 0L
        val files = Vector.newBuilder[String]
        for (file <- f) {
          elementCount += file.tags(TagUtils.elementCount).toLong
          val maxWeight = file.tags(TagUtils.maxWeight).toInt
          if (maxWeight < minMaxWeight) {
            minMaxWeight = maxWeight
          }
          files += file.path
        }
        val cubeStatus = if (minMaxWeight == Int.MaxValue) {
          CubeStatus(
            Weight.MaxValue,
            NormalizedWeight(rev.desiredCubeSize, elementCount),
            files.result())
        } else {
          val w = Weight(minMaxWeight)
          CubeStatus(w, NormalizedWeight(w), files.result())
        }
        (rev.createCubeId(cube), cubeStatus)
      })
      .collect()
      .foreach(builder += _)
    builder.result()
  }

}
