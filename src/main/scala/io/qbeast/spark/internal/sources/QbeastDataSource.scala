/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.context.QbeastContext
import io.qbeast.spark.table.IndexedTableFactory
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider,
  InsertableRelation
}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{
  AnalysisExceptionFactory,
  DataFrame,
  SQLContext,
  SaveMode,
  SparkSession
}

import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec}
import io.qbeast.spark.delta.OTreeIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import java.util
import scala.collection.JavaConverters._
import org.apache.spark.sql.execution.datasources

/**
 * Qbeast data source is implementation of Spark data source API V1.
 */
class QbeastDataSource private[sources] (private val tableFactory: IndexedTableFactory)
    extends TableProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with RelationProvider
    with InsertableRelation {

  /**
   * Constructor to be used by Spark.
   */
  def this() = this(QbeastContext.indexedTableFactory)

  override def shortName(): String = "qbeast"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = StructType(Seq())

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = new TableImpl(schema)

  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    files.head.getPath.getParent
    Some(createRelation(sparkSession.sqlContext, options).schema)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    require(
      parameters.contains("columnsToIndex"),
      throw AnalysisExceptionFactory.create("'columnsToIndex is not specified"))
    val tableId = SparkToQTypesUtils.loadFromParameters(parameters)
    val table = tableFactory.getIndexedTable(tableId)
    mode match {
      case SaveMode.Append => table.save(data, parameters, append = true)
      case SaveMode.Overwrite => table.save(data, parameters, append = false)
      case SaveMode.ErrorIfExists if table.exists =>
        throw AnalysisExceptionFactory.create(s"The table '$tableId' already exists.")
      case SaveMode.ErrorIfExists => table.save(data, parameters, append = false)
      case SaveMode.Ignore if table.exists => table.load()
      case SaveMode.Ignore => table.save(data, parameters, append = false)
    }
  }

  /**
   * Returns a [[QbeastBaseRelation]] that contains all of the data present
   * in the table. This relation will be continually updated
   * as files are added or removed from the table. However, new [[QbeastBaseRelation]]
   * must be requested in order to see changes to the schema.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val spark = SparkSession.active
    val tableID = SparkToQTypesUtils.loadFromParameters(parameters)
    val table = tableFactory.getIndexedTable(tableID)
    val log = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID)

    val path = tableID.asInstanceOf[Path]
    val tahoe = TahoeLogFileIndex(
      spark,
      log.deltaLog,
      path,
      log.deltaLog.snapshot,
      Seq.empty,
      false
    ) // TODO get time_travel boolean from somewhere
    val fileIndex = OTreeIndex(tahoe)
    val bucketSpec: Option[BucketSpec] = None

    // TODO understand how to use fileFormat
    // val file = ParquetFileFormat.readSchemaFromFooter(log.qbeastSnapshot)

    new HadoopFsRelation(
      fileIndex,
      partitionSchema = StructType(Seq.empty[StructField]),
      dataSchema = log.createRelation.schema,
      bucketSpec = bucketSpec,
      fileFormat(log.qbeastSnapshot),
      // `metadata.format.options` is not set today. Even if we support it in future, we shouldn't
      // store any file system options since they may contain credentials. Hence, it will never
      // conflict with `DeltaLog.options`.
      parameters)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
        mode match {
          case SaveMode.Append => table.save(data, parameters, append = true)
          case SaveMode.Overwrite => table.save(data, parameters, append = false)
        }
      }
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val tableID = SparkToQTypesUtils.loadFromParameters(parameters)
    val table = tableFactory.getIndexedTable(tableID)
    if (table.exists) {
      table.load()
    } else {
      throw AnalysisExceptionFactory.create(
        s"'$tableID' is not a Qbeast formatted data directory.")
    }
  }

}

private class TableImpl(schema: StructType) extends Table {
  override def name(): String = "qbeast"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

}
