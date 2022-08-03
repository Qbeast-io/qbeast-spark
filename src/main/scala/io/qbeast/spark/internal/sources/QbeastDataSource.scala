/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.context.QbeastContext
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.internal.QbeastOptions.checkQbeastProperties
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.{FileStatus, Path}
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

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = StructType(Seq())

  // Used to get the table of an existing one
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val tableId = QbeastOptions.loadTableIDFromParameters(properties.asScala.toMap)
    new QbeastTableImpl(
      new Path(tableId.id),
      properties.asScala.toMap,
      Some(schema),
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

    checkQbeastProperties(parameters)
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
    val indexedTable = tableFactory.getIndexedTable(tableID)

    // If the table has data registered on the snapshot, we can load from the IndexedTable factory
    // Otherwise, the table can be loaded from the catalog
    if (indexedTable.exists) indexedTable.load()
    else {
      // If indexedTable does not contain data
      // Check if it's registered on the catalog
      val tableImpl = new QbeastTableImpl(new Path(tableID.id), parameters, None, tableFactory)
      if (tableImpl.isCatalogTable) { tableImpl.toBaseRelation }
      else {
        throw AnalysisExceptionFactory.create(
          s"'$tableID' is not a Qbeast formatted data directory.")
      }
    }
  }

}
