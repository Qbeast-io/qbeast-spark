/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.sources

import io.qbeast.context.QbeastContext
import io.qbeast.spark.index.ColumnsToIndex
import io.qbeast.spark.sql.qbeast.QbeastOptions
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{
  AnalysisExceptionFactory,
  DataFrame,
  SQLContext,
  SaveMode,
  SparkSession
}

import java.util
import scala.collection.JavaConverters._

/**
 * Qbeast data source is implementation of Spark data source API V1.
 */
class QbeastDataSource private[sources] (private val tableFactory: IndexedTableFactory)
    extends TableProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with RelationProvider {

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
    Some(
      new DeltaDataSource()
        .createRelation(
          sparkSession.sqlContext,
          options + ("path" -> files.head.getPath.getParent.toString))
        .schema)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = getPath(parameters)
    val table = tableFactory.getIndexedTable(sqlContext, path)
    val columnsToIndex = getColumnsToIndex(parameters)
    mode match {
      case SaveMode.Append => table.save(data, columnsToIndex, append = true)
      case SaveMode.Overwrite => table.save(data, columnsToIndex, append = false)
      case SaveMode.ErrorIfExists if table.exists =>
        throw AnalysisExceptionFactory.create(s"The table '$path' already exists.")
      case SaveMode.ErrorIfExists => table.save(data, columnsToIndex, append = false)
      case SaveMode.Ignore if table.exists => table.load()
      case SaveMode.Ignore => table.save(data, columnsToIndex, append = false)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path = getPath(parameters)
    val table = tableFactory.getIndexedTable(sqlContext, path)
    if (table.exists) {
      table.load()
    } else {
      throw AnalysisExceptionFactory.create(s"'$path' is not a Qbeast formatted data directory.")
    }
  }

  private def getPath(parameters: Map[String, String]): String = {
    parameters.getOrElse(
      "path", {
        throw AnalysisExceptionFactory.create("'path' is not specified")
      })
  }

  private def getColumnsToIndex(parameters: Map[String, String]): Seq[String] = {
    val encodedColumnsToIndex = parameters.getOrElse(
      QbeastOptions.COLUMNS_TO_INDEX, {
        throw AnalysisExceptionFactory.create(
          "you must specify the columns to index in a comma separated way" +
            " as .option(columnsToIndex, ...)")
      })
    ColumnsToIndex.decode(encodedColumnsToIndex)
  }

}

private class TableImpl(schema: StructType) extends Table {
  override def name(): String = "qbeast"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

}
