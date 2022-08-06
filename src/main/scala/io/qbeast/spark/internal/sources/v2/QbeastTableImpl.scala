/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.context.QbeastContext._
import io.qbeast.core.model.QTableID
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.connector.catalog.TableCapability._
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.{AnalysisExceptionFactory, SparkSession, V2toV1Fallback}
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters._

/**
 * Table Implementation for Qbeast Format
 * @param path the Path of the table
 * @param options the write options
 * @param schema the schema of the table
 * @param catalogTable the underlying Catalog Table, if any
 * @param tableFactory the IndexedTable Factory
 */
class QbeastTableImpl private[sources] (
    path: Path,
    options: Map[String, String],
    schema: Option[StructType] = None,
    catalogTable: Option[CatalogTable] = None,
    private val tableFactory: IndexedTableFactory)
    extends Table
    with SupportsWrite
    with V2toV1Fallback {

  private lazy val spark = SparkSession.active

  private val pathString = path.toString

  private val tableId = QTableID(pathString)

  private val indexedTable = tableFactory.getIndexedTable(tableId)

  override def name(): String = tableId.id

  override def schema(): StructType = {
    if (schema.isDefined) schema.get
    else metadataManager.loadCurrentSchema(tableId)
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

  // Returns the write builder for the query in info
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new QbeastWriteBuilder(info, options, indexedTable)
  }

  def toBaseRelation: BaseRelation = {
    QbeastBaseRelation.forQbeastTable(indexedTable)
  }

  override def properties(): util.Map[String, String] = options.asJava

  override def v1Table: CatalogTable = {
    if (catalogTable.isDefined) catalogTable.get
    else throw AnalysisExceptionFactory.create("No catalog table defined")
  }

}
