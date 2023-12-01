/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.catalog

import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.SparkCatalogV2Util
import org.apache.spark.sql.connector.catalog.StagedTable
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory

/**
 * A default StagedTable This case class would delegate the methods to the underlying Catalog
 * Table
 * @param ident
 *   the identifier
 * @param table
 *   the Table
 * @param catalog
 *   the Catalog
 */
private[catalog] case class DefaultStagedTable(
    ident: Identifier,
    table: Table,
    catalog: TableCatalog)
    extends SupportsWrite
    with StagedTable {

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    table match {
      case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
      case _ =>
        throw AnalysisExceptionFactory.create(s"Table `${ident.name}` does not support writes.")
    }
  }

  override def abortStagedChanges(): Unit = catalog.dropTable(ident)

  override def commitStagedChanges(): Unit = {}

  override def name(): String = ident.name()

  override def schema(): StructType = SparkCatalogV2Util.v2ColumnsToStructType(columns())

  override def columns(): Array[Column] = table.columns()

  override def partitioning(): Array[Transform] = table.partitioning()

  override def capabilities(): java.util.Set[TableCapability] = table.capabilities()

  override def properties(): java.util.Map[String, String] = table.properties()
}
