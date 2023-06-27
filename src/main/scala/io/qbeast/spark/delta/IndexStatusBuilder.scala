/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.spark.delta.QbeastMetadataSQL._
import io.qbeast.spark.utils.State.FLOODED
import io.qbeast.spark.utils.TagColumns
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.sum

import scala.collection.immutable.SortedMap

/**
 * Builds the index status from a given snapshot and revision
 * @param qbeastSnapshot the QbeastSnapshot
 * @param revision the revision
 * @param announcedSet the announced set available for the revision
 * @param replicatedSet the replicated set available for the revision
 */
private[delta] class IndexStatusBuilder(qbeastSnapshot: DeltaQbeastSnapshot, revision: Revision)
    extends Serializable
    with StagingUtils {

  /**
   * Dataset of files belonging to the specific revision
   * @return the dataset of AddFile actions
   */
  def revisionFiles: Dataset[AddFile] =
    // this must be external to the lambda, to avoid SerializationErrors
    qbeastSnapshot.loadRevisionBlocks(revision.revisionID)

  def build(): IndexStatus = {
    val cubeStatuses =
      if (isStaging(revision)) stagingCubeStatuses
      else buildCubesStatuses

    val (replicatedSet, announcedSet): (Set[CubeId], Set[CubeId]) =
      if (isStaging(revision)) (Set.empty, Set.empty)
      else buildReplicatedAndAnnouncedSets(cubeStatuses)

    IndexStatus(
      revision = revision,
      replicatedSet = replicatedSet,
      announcedSet = announcedSet,
      cubesStatuses = cubeStatuses)
  }

  def stagingCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val root = revision.createCubeIdRoot()
    val maxWeight = Weight.MaxValue
    val blocks = revisionFiles
      .collect()
      .map(addFile =>
        QbeastBlock(
          addFile.path,
          "",
          revision.revisionID,
          Weight.MinValue,
          maxWeight,
          FLOODED,
          false,
          0,
          addFile.size,
          addFile.modificationTime))
      .toIndexedSeq

    SortedMap(root -> CubeStatus(root, maxWeight, maxWeight.fraction, blocks))
  }

  /**
   * Returns the index state for the given space revision
   * @return Dataset containing cube information
   */
  def buildCubesStatuses: SortedMap[CubeId, CubeStatus] = {

    val spark = SparkSession.active
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]

    val rev = revision

    import spark.implicits._
    val ndims: Int = rev.transformations.size
    revisionFiles
      .groupBy(TagColumns.cube)
      .agg(
        min(weight(TagColumns.maxWeight)).as("maxWeight"),
        sum(TagColumns.elementCount).as("elementCount"),
        collect_list(qblock).as("files"))
      .select(
        createCube(col("cube"), lit(ndims)).as("cubeId"),
        col("maxWeight"),
        normalizeWeight(col("maxWeight"), col("elementCount"), lit(rev.desiredCubeSize))
          .as("normalizedWeight"),
        col("files"))
      .as[CubeStatus]
      .collect()
      .foreach(row => builder += row.cubeId -> row)
    builder.result()
  }

  def buildReplicatedAndAnnouncedSets(
      cubeStatuses: Map[CubeId, CubeStatus]): (Set[CubeId], Set[CubeId]) = {
    val replicatedSet = Set.newBuilder[CubeId]
    val announcedSet = Set.newBuilder[CubeId]
    cubeStatuses.foreach { case (id, status) =>
      var hasReplicated = false
      var hasUnreplicated = false
      status.files.foreach(file =>
        if (file.replicated) hasReplicated = true
        else hasUnreplicated = true)
      if (hasReplicated) {
        if (hasUnreplicated) {
          announcedSet += id
        } else {
          replicatedSet += id
        }
      }
    }
    (replicatedSet.result(), announcedSet.result())
  }

}
