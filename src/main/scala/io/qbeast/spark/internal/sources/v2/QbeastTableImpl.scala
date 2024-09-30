/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableId
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCapability
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.V2toV1Fallback

import java.util
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Table Implementation for Qbeast Format
 * @param path
 *   the Path of the table
 * @param options
 *   the write options
 * @param optionSchema
 *   the schema of the table
 * @param catalogTable
 *   the underlying Catalog Table, if any
 * @param tableFactory
 *   the IndexedTable Factory
 */
case class QbeastTableImpl(
    tableIdentifier: TableIdentifier,
    path: Path,
    options: Map[String, String] = Map.empty,
    optionSchema: Option[StructType] = None,
    catalogTable: Option[CatalogTable] = None,
    private val tableFactory: IndexedTableFactory)
    extends Table
    with SupportsWrite
    with V2toV1Fallback {

  private val pathString = path.toString

  private val tableId = QTableId(pathString)

  private val indexedTable = tableFactory.getIndexedTable(tableId)

  private lazy val spark = SparkSession.active

  private lazy val initialSnapshot = QbeastContext.metadataManager.loadSnapshot(tableId)

  private lazy val table: CatalogTable =
    if (catalogTable.isDefined) catalogTable.get
    else {
      // Get table Metadata if no catalog table is provided
      spark.sessionState.catalog
        .getTableMetadata(tableIdentifier)
    }

  override def name(): String = tableIdentifier.identifier

  override def schema(): StructType =
    if (optionSchema.isDefined) optionSchema.get else table.schema

  override def capabilities(): util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

  // Returns the write builder for the query in info
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new QbeastWriteBuilder(info, options, indexedTable)
  }

  def toBaseRelation: BaseRelation = {
    QbeastBaseRelation.forQbeastTableWithOptions(indexedTable, properties().asScala.toMap)
  }

  override def properties(): util.Map[String, String] = {
    val description = initialSnapshot.loadDescription
    val base = mutable.Map() ++ initialSnapshot.loadProperties
    options.foreach {
      case (key, value) if !base.contains(key) =>
        base.put(key, value)
      case _ => // do nothing
    }
    base.put(TableCatalog.PROP_PROVIDER, "qbeast")
    base.put(TableCatalog.PROP_LOCATION, CatalogUtils.URIToString(path.toUri))
    catalogTable.foreach { table =>
      if (table.owner != null && table.owner.nonEmpty) {
        base.put(TableCatalog.PROP_OWNER, table.owner)
      }
      v1Table.storage.properties.foreach { case (key, value) =>
        base.put(TableCatalog.OPTION_PREFIX + key, value)
      }
      if (v1Table.tableType == CatalogTableType.EXTERNAL) {
        base.put(TableCatalog.PROP_EXTERNAL, "true")
      }
    }
    Option(description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    base.asJava
  }

  override def v1Table: CatalogTable = table

}
