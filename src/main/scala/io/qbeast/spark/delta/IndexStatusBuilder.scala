/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.spark.delta.IndexStatusBuilder.{createCube, norm, qblock, weight}
import io.qbeast.spark.utils.TagColumns
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, collect_list, lit, min, struct, sum, udf}
import org.apache.spark.sql.{Column, Dataset, SparkSession}

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
  def revisionFiles: Dataset[AddFile] =
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

    val spark = SparkSession.active
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]

    val rev = revision

    import spark.implicits._
    val ndims: Int = rev.transformations.size
    // TODO some files may not include metadata
    val filesWithMetadata = revisionFiles.where("tags is not null")
    filesWithMetadata
      .groupBy(TagColumns.cube)
      .agg(
        weight(min(TagColumns.maxWeight)).as("maxWeight"),
        sum(TagColumns.elementCount).as("elementCount"),
        collect_list(qblock).as("files"))
      .select(
        createCube(col("cube"), lit(ndims)).as("cubeId"),
        col("maxWeight"),
        norm(col("maxWeight"), col("elementCount"), lit(rev.desiredCubeSize)).as(
          "normalizedWeight"),
        col("files"))
      .as[CubeStatus]
      .collect()
      .foreach(row => builder += row.cubeId -> row)
    builder.result()
  }

}

object IndexStatusBuilder {
  val weight: UserDefinedFunction = udf((weight: Int) => Weight(weight))

  val norm: UserDefinedFunction = udf((mw: Weight, elementCount: Long, desiredSize: Int) =>
    if (mw < Weight.MaxValue) {
      mw.fraction
    } else {
      NormalizedWeight.apply(desiredSize, elementCount)
    })

  val createCube: UserDefinedFunction =
    udf((cube: String, dimensions: Int) => CubeId(dimensions, cube))

  val qblock: Column =
    struct(
      col("path"),
      col("size"),
      col("modificationTime"),
      weight(TagColumns.minWeight).as("minWeight"),
      weight(TagColumns.maxWeight)
        .as("maxWeight"),
      TagColumns.state,
      TagColumns.revision.cast("bigint").as("revision"),
      TagColumns.elementCount.cast("bigint").as("elementCount"))

}
