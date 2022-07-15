/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

// import io.qbeast.IISeq
import io.qbeast.core.model.{QTableID} // , Revision}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
// import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.sources.BaseRelation

import org.apache.spark.sql.sources.InsertableRelation
// import org.apache.hadoop.conf.Configuration

// import org.apache.spark.sql.delta.DeltaFileFormat

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SparkSession
import io.qbeast.spark.delta.OTreeIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import io.qbeast.spark.table.IndexedTable
// import io.qbeast.spark.utils.SparkToQTypesUtils
// import org.apache.spark.sql.execution.datasources.FileFormat

// import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec}
// import org.apache.spark.sql.{
//   AnalysisExceptionFactory,
//   DataFrame,
//   SQLContext,
//   SaveMode,
//   SparkSession
// }
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

/**
 * Companion object for QbeastBaseRelation
 */
object QbeastBaseRelation {

  /**
   * Creates a QbeastBaseRelation instance
   * @param tableID the identifier of the table
   * @return the QbeastBaseRelation
   */

  /**
   * Returns a [[QbeastBaseRelation]] that contains all of the data present
   * in the table. This relation will be continually updated
   * as files are added or removed from the table. However, new [[QbeastBaseRelation]]
   * must be requested in order to see changes to the schema.
   */
  def createRelation(sqlContext: SQLContext, table: IndexedTable): BaseRelation = {

    val spark = SparkSession.active
    val tableID = table.tableID
    val log = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID)
    val revision = log.qbeastSnapshot.loadLatestRevision
    val columnsToIndex = revision.columnTransformers.map(row => row.columnName).mkString(",")
    val cubeSize = revision.desiredCubeSize
    val parameters = Map[String, String](
      "path" -> table.tableID.toString(),
      "columnsToIndex" -> columnsToIndex,
      "cubeSize" -> cubeSize.toString())
    // val tableID = SparkToQTypesUtils.loadFromParameters(parameters)

    // val path = new Path(tableID)
    // Data path for tableID
    val path = log.deltaLog.dataPath
    val tahoe =
      TahoeLogFileIndex(spark, log.deltaLog, path, log.deltaLog.snapshot, Seq.empty, false)
    val fileIndex = OTreeIndex(tahoe)
    val bucketSpec: Option[BucketSpec] = None

    val file = new ParquetFileFormat()
    // val file = fileFormat(log.qbeastSnapshot)

    new HadoopFsRelation(
      fileIndex,
      partitionSchema = StructType(Seq.empty[StructField]),
      dataSchema = log.createRelation.schema,
      bucketSpec = bucketSpec,
      file,
      parameters)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        // val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        table.save(data, parameters, append = !overwrite)
      }
    }
  }

  def forDeltaTable(tableID: QTableID, indexedTable: IndexedTable): BaseRelation = {

    val spark = SparkSession.active

    createRelation(spark.sqlContext, indexedTable)

  }

}
