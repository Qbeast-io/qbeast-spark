/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model._
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.{Dataset, SparkSession}

private[delta] class IndexStatusBuilder(
    qbeastSnapshot: DeltaQbeastSnapshot,
    revision: Revision,
    announcedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  def revisionFiles: Dataset[AddFile] = {
    // this must be external to the lambda, to avoid SerializationErrors
    val revID = revision.revisionID.toString
    qbeastSnapshot.snapshot.allFiles.filter(_.tags(TagUtils.revision) == revID)
  }

  def replicatedSet: ReplicatedSet =
    qbeastSnapshot.getReplicatedSet(revision.revisionID)

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
  def buildCubesStatuses: Map[CubeId, CubeStatus] = {

    val spark = SparkSession.active
    import spark.implicits._
    val rev = revision
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
      .toMap
  }

}
