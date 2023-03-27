/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.context.QbeastContext
import io.qbeast.context.QbeastContext.metadataManager
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
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
import scala.collection.JavaConverters.mapAsScalaMapConverter

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

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val parameters = options.asScala.toMap
    val tableId = QbeastOptions.loadTableIDFromParameters(parameters)
    QbeastContext.metadataManager.loadCurrentSchema(tableId)
  }

  // Used to get the table of an existing one
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    var tableProperties = properties.asScala.toMap
    val tableId = QbeastOptions.loadTableIDFromParameters(tableProperties)
    val indexedTable = tableFactory.getIndexedTable(tableId)
    if (indexedTable.exists) {
      // If the table exists, we make sure to pass all the properties to QbeastTableImpl
      val currentRevision = metadataManager.loadSnapshot(tableId).loadLatestRevision
      val builder = Map.canBuildFrom[String, String](tableProperties)
      builder += "columnsToIndex" -> currentRevision.columnTransformers
        .map(_.columnName)
        .mkString(",")
      builder += "cubeSize" -> currentRevision.desiredCubeSize.toString
      tableProperties = builder.result()
    }
    new QbeastTableImpl(
      TableIdentifier(tableId.id),
      new Path(tableId.id),
      tableProperties,
      Some(schema),
      None,
      tableFactory)
  }

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
      parameters.contains("columnsToIndex") || mode == SaveMode.Append,
      throw AnalysisExceptionFactory.create("'columnsToIndex' is not specified"))

    val tableId = QbeastOptions.loadTableIDFromParameters(parameters)
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

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val tableID = QbeastOptions.loadTableIDFromParameters(parameters)
    val table = tableFactory.getIndexedTable(tableID)
    if (table.exists) {
      table.load()
    } else {
      throw AnalysisExceptionFactory.create(
        s"'$tableID' is not a Qbeast formatted data directory.")
    }
  }

}
