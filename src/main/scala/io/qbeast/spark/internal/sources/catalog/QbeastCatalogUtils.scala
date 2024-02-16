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
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.context.QbeastContext.{metadataManager}
import io.qbeast.core.model.QTableID
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.connector.catalog.{Identifier, SparkCatalogV2Util, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{
  AnalysisExceptionFactory,
  DataFrame,
  SaveMode,
  SparkSession,
  V1TableQbeast
}

import java.util
import scala.collection.JavaConverters._

/**
 * Object containing all the method utilities for creating and loading
 * a Qbeast formatted Table into the Catalog
 */
object QbeastCatalogUtils {

  val QBEAST_PROVIDER_NAME: String = "qbeast"

  lazy val spark: SparkSession = SparkSession.active

  /**
   * Checks if the provider is Qbeast
   * @param provider the provider, if any
   * @return
   */
  def isQbeastProvider(provider: Option[String]): Boolean = {
    provider.isDefined && provider.get == QBEAST_PROVIDER_NAME
  }

  def isQbeastProvider(tableSpec: TableSpec): Boolean = {
    tableSpec.provider.contains(QBEAST_PROVIDER_NAME)
  }

  def isQbeastProvider(properties: Map[String, String]): Boolean = isQbeastProvider(
    properties.get("provider"))

  def isQbeastProvider(properties: util.Map[String, String]): Boolean = isQbeastProvider(
    properties.asScala.toMap)

  /**
   * Checks if an Identifier is set with a path
   * @param ident the Identifier
   * @return
   */
  def isPathTable(ident: Identifier): Boolean = {
    new Path(ident.name()).isAbsolute
  }

  def isPathTable(identifier: TableIdentifier): Boolean = {
    isPathTable(Identifier.of(identifier.database.toArray, identifier.table))
  }

  /** Checks if a table already exists for the provided identifier. */
  def getExistingTableIfExists(
      table: TableIdentifier,
      existingSessionCatalog: SessionCatalog): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    if (isPathTable(table)) return None
    val tableExists = existingSessionCatalog.tableExists(table)
    if (tableExists) {
      val oldTable = existingSessionCatalog.getTableMetadata(table)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw AnalysisExceptionFactory.create(
          s"$table is a view. You may not write data into a view.")
      }
      if (!isQbeastProvider(oldTable.provider)) {
        throw AnalysisExceptionFactory.create(s"$table is not a Qbeast table.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  private def verifySchema(fs: FileSystem, path: Path, table: CatalogTable): CatalogTable = {

    val isTablePopulated = table.tableType == CatalogTableType.EXTERNAL && fs
      .exists(path) && fs.listStatus(path).nonEmpty
    // Users did not specify the schema. We expect the schema exists in Delta.
    if (table.schema.isEmpty) {
      if (table.tableType == CatalogTableType.EXTERNAL) {
        if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
          val existingSchema =
            DeltaLog.forTable(spark, path.toString).unsafeVolatileSnapshot.metadata.schema
          table.copy(schema = existingSchema)
        } else {
          throw AnalysisExceptionFactory
            .create(
              "Trying to create an External Table without any schema. " +
                "Please specify the schema in the command or use a path of a populated table.")
        }
      } else {
        throw AnalysisExceptionFactory
          .create(
            "Trying to create a managed table without schema. " +
              "Do you want to create it as EXTERNAL?")
      }
    } else {
      if (isTablePopulated) {
        val existingSchema =
          DeltaLog.forTable(spark, path.toString).unsafeVolatileSnapshot.metadata.schema
        if (existingSchema != table.schema) {
          throw AnalysisExceptionFactory
            .create(
              "Trying to create a managed table with a different schema. " +
                "Do you want to want to ALTER TABLE first?")
        }
      }
      table
    }
  }

  /**
   * Creates a Table on the Catalog
   * @param ident the Identifier of the table
   * @param schema the schema of the table
   * @param partitions the partitions of the table, if any
   * @param allTableProperties all the table properties
   * @param writeOptions the write properties of the table
   * @param dataFrame the dataframe to write, if any
   * @param tableCreationMode the creation mode (could be CREATE, REPLACE or CREATE OR REPLACE)
   * @param tableFactory the indexed table factory
   * @param existingSessionCatalog the existing session catalog
   */

  def createQbeastTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      allTableProperties: util.Map[String, String],
      writeOptions: Map[String, String],
      dataFrame: Option[DataFrame],
      tableCreationMode: CreationMode,
      tableFactory: IndexedTableFactory,
      existingSessionCatalog: SessionCatalog): Unit = {

    val isPathTable = this.isPathTable(ident)
    val properties = allTableProperties.asScala.toMap

    // Get table location
    val location = if (isPathTable) {
      Option(ident.name())
    } else {
      properties.get("location")
    }

    // Define the table type.
    // Either can be EXTERNAL (if the location is defined) or MANAGED
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val locUriOpt = location.map(CatalogUtils.stringToURI)

    val id = TableIdentifier(ident.name(), ident.namespace().lastOption)
    val existingTableOpt = QbeastCatalogUtils.getExistingTableIfExists(id, existingSessionCatalog)
    val loc = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(existingSessionCatalog.defaultTablePath(id))

    // Process the parameters/options/configuration sent to the table
    val indexedTable = tableFactory.getIndexedTable(QTableID(loc.toString))
    val allProperties = indexedTable.verifyAndMergeProperties(properties)

    // Initialize the path option
    val storage = DataSource
      .buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val commentOpt = Option(allTableProperties.get("comment"))

    if (partitions.nonEmpty) {
      throw AnalysisExceptionFactory
        .create(
          "Qbeast Format does not support partitioning/bucketing. " +
            "You may still want to use your partition columns as columnsToIndex " +
            "to get all the benefits of data skipping. ")
    }

    val t = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some("qbeast"),
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = allProperties,
      comment = commentOpt)

    // Verify the schema if it's an external table
    val tableLocation = new Path(loc)
    val hadoopConf = spark.sharedState.sparkContext.hadoopConfiguration
    val fs = tableLocation.getFileSystem(hadoopConf)
    val table = verifySchema(fs, tableLocation, t)

    // Write data, if any
    val append = tableCreationMode.saveMode == SaveMode.Append
    dataFrame.map { df =>
      indexedTable.save(df, allProperties, append)
    }

    // Update the existing session catalog with the Qbeast table information
    updateCatalog(
      QTableID(loc.toString),
      tableCreationMode,
      table,
      isPathTable,
      existingTableOpt,
      existingSessionCatalog)
  }

  private def checkLogCreation(tableID: QTableID): Unit = {
    // If the Log is not created
    // We make sure we create the table physically
    // So new data can be inserted
    val isLogCreated = metadataManager.existsLog(tableID)
    if (!isLogCreated) metadataManager.createLog(tableID)
  }

  /**
   * Based on DeltaCatalog updateCatalog private method,
   * it maintains the consistency of creating a table
   * calling the spark session catalog.
   * @param operation
   * @param table
   * @param isPathTable
   * @param existingTableOpt
   * @param existingSessionCatalog
   */
  private def updateCatalog(
      tableID: QTableID,
      operation: CreationMode,
      table: CatalogTable,
      isPathTable: Boolean,
      existingTableOpt: Option[CatalogTable],
      existingSessionCatalog: SessionCatalog): Unit = {

    operation match {
      case _ if isPathTable => // do nothing
      case TableCreationMode.CREATE_TABLE =>
        // To create the table, check if the log exists/create a new one
        // create table in the SessionCatalog
        checkLogCreation(tableID)
        existingSessionCatalog.createTable(
          table,
          ignoreIfExists = existingTableOpt.isDefined,
          validateLocation = false)
      case TableCreationMode.REPLACE_TABLE | TableCreationMode.CREATE_OR_REPLACE
          if existingTableOpt.isDefined =>
        // REPLACE the metadata of the table with the new one
        existingSessionCatalog.alterTable(table)
      case TableCreationMode.REPLACE_TABLE =>
        // Throw an exception if the table to replace does not exists
        val ident = Identifier.of(table.identifier.database.toArray, table.identifier.table)
        throw new CannotReplaceMissingTableException(ident)
      case TableCreationMode.CREATE_OR_REPLACE =>
        checkLogCreation(tableID)
        existingSessionCatalog.createTable(
          table,
          ignoreIfExists = false,
          validateLocation = false)
    }
  }

  /**
   * Loads a qbeast table based on the underlying table
   * @param table the underlying table
   * @return a Table with Qbeast information and implementations
   */
  def loadQbeastTable(table: Table, tableFactory: IndexedTableFactory): Table = {

    val prop = table.properties()
    val columns = table.columns()
    val schema = SparkCatalogV2Util.v2ColumnsToStructType(columns)

    table match {
      case V1TableQbeast(t) =>
        val catalogTable = t.v1Table

        val path: String = if (catalogTable.tableType == CatalogTableType.EXTERNAL) {
          // If it's an EXTERNAL TABLE, we can find the path through the Storage Properties
          catalogTable.storage.locationUri.get.toString
        } else {
          // If it's a MANAGED TABLE, the location is set in the former catalogTable
          catalogTable.location.toString
        }

        new QbeastTableImpl(
          catalogTable.identifier,
          new Path(path),
          prop.asScala.toMap,
          Some(schema),
          Some(catalogTable),
          tableFactory)

      case _ => table
    }
  }

}
