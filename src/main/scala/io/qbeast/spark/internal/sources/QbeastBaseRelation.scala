/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.context.QbeastContext
import io.qbeast.spark.delta.DefaultFileIndex
import io.qbeast.spark.delta.EmptyFileIndex
import io.qbeast.spark.table.IndexedTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

/**
 * Companion object for QbeastBaseRelation
 */
object QbeastBaseRelation {

  /**
   * Returns a HadoopFsRelation that contains all of the data present in the table. This relation
   * will be continually updated as files are added or removed from the table. However, new
   * HadoopFsRelation must be requested in order to see changes to the schema.
   * @param tableID
   *   the identifier of the table
   * @param sqlContext
   *   the SQLContext
   * @return
   *   the HadoopFsRelation
   */
  def createRelation(
      sqlContext: SQLContext,
      table: IndexedTable,
      options: Map[String, String]): BaseRelation = {

    val spark = SparkSession.active
    val tableID = table.tableID
    val snapshot = QbeastContext.metadataManager.loadSnapshot(tableID)
    val schema = QbeastContext.metadataManager.loadCurrentSchema(tableID)
    if (snapshot.isInitial) {
      // If the Table is initial, read empty relation
      // This could happen if we CREATE/REPLACE TABLE without inserting data
      // In this case, we use the options variable
      new HadoopFsRelation(
        EmptyFileIndex,
        partitionSchema = StructType(Seq.empty[StructField]),
        dataSchema = schema,
        bucketSpec = None,
        new ParquetFileFormat(),
        options)(spark) with InsertableRelation {
        def insert(data: DataFrame, overwrite: Boolean): Unit = {
          table.save(data, options, append = !overwrite)
        }
      }
    } else {
      // If the table contains data, initialize it
      val revision = snapshot.loadLatestRevision
      val columnsToIndex = revision.columnTransformers.map(row => row.columnName).mkString(",")
      val cubeSize = revision.desiredCubeSize
      val parameters =
        Map[String, String]("columnsToIndex" -> columnsToIndex, "cubeSize" -> cubeSize.toString())

      val path = new Path(tableID.id)
      val fileIndex = DefaultFileIndex(spark, path)
      val bucketSpec: Option[BucketSpec] = None
      val file = new ParquetFileFormat()

      new HadoopFsRelation(
        fileIndex,
        partitionSchema = StructType(Seq.empty[StructField]),
        dataSchema = schema,
        bucketSpec = bucketSpec,
        file,
        parameters)(spark) with InsertableRelation {
        def insert(data: DataFrame, overwrite: Boolean): Unit = {
          table.save(data, parameters, append = !overwrite)
        }
      }
    }
  }

  /**
   * Function that can be called from a QbeastBaseRelation object to create a new
   * QbeastBaseRelation with a new tableID.
   * @param indexedTable
   *   the indexed table
   * @return
   *   BaseRelation for the new table in Qbeast format
   */
  def forQbeastTable(indexedTable: IndexedTable): BaseRelation = {
    forQbeastTableWithOptions(indexedTable, Map.empty)
  }

  def forQbeastTableWithOptions(
      indexedTable: IndexedTable,
      withOptions: Map[String, String]): BaseRelation = {
    val spark = SparkSession.active
    createRelation(spark.sqlContext, indexedTable, withOptions)
  }

}
