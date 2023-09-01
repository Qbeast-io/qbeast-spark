/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.{CubeId, QTableID, Revision}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import io.qbeast.spark.internal.sources.QbeastFileFormat
import io.qbeast.context.QbeastContext
import org.apache.hadoop.fs.Path

/**
 * Loads cube data from a specific table
 * @param tableID the table identifier
 */
case class CubeDataLoader(tableID: QTableID) {
  private val spark = SparkSession.active
  private val metadataManager = QbeastContext.metadataManager

  /**
   * Loads the data from a set of cubes in a specific revision
   * and adds column information
   * @param cubeSet the set of cubes to load
   * @param revision the revision to load
   * @param columnName the column name to add
   * @return the dataframe
   */
  def loadSetWithCubeColumn(
      cubeSet: Set[CubeId],
      revision: Revision,
      columnName: String): DataFrame = {
    cubeSet
      .map(loadWithCubeColumn(_, revision, columnName))
      .filter(df => !df.isEmpty)
      .reduceOption(_ union _)
      .getOrElse(spark.emptyDataFrame)
  }

  /**
   * Loads the data from a single cube in a specific revision
   * and adds column information
   * @param cubeId the cube to load
   * @param revision the revision to load
   * @param columnName the column name to add
   * @return the dataframe
   */
  def loadWithCubeColumn(cubeId: CubeId, revision: Revision, columnName: String): DataFrame = {
    loadCubeData(cubeId, revision).withColumn(columnName, lit(cubeId.bytes))
  }

  /**
   * Loads the data from a single cube in a specific revision
   *
   * @param cubeId the cube identifier
   * @param revision the revision to load
   * @return the dataframe without extra information
   */
  def loadCubeData(cubeId: CubeId, revision: Revision): DataFrame = {
    val path = new Path(tableID.id)
    val snapshot = metadataManager.loadSnapshot(tableID).asInstanceOf[DeltaQbeastSnapshot]
    val index = CubeIndex(spark, path, snapshot, revision.revisionID, cubeId)
    val partitionSchema = StructType(Seq.empty[StructField])
    val dataSchema = metadataManager.loadCurrentSchema(tableID)
    val bucketSpec = None
    val fileFormat = new QbeastFileFormat()
    val options = Map.empty[String, String]
    val relation =
      HadoopFsRelation(index, partitionSchema, dataSchema, bucketSpec, fileFormat, options)(spark)
    spark.baseRelationToDataFrame(relation)
  }

}
