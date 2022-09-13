/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.core.model.QTableID
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{
  AnalysisExceptionFactory,
  DataFrame,
  SaveMode,
  SparkSession,
  SparkTransformUtils,
  V1TableQbeast
}

import java.util
import java.util.Locale
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

  /**
   * Checks if an Identifier is set with a path
   * @param ident the Identifier
   * @return
   */
  def isPathTable(ident: Identifier): Boolean = {
    try {
      spark.sessionState.conf.runSQLonFile && hasQbeastNamespace(ident) && new Path(
        ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
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

  /**
   * Checks if the identifier has namespace of Qbeast
   * @param ident
   * @return
   */
  def hasQbeastNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && ident.name.toLowerCase(Locale.ROOT) == QBEAST_PROVIDER_NAME
  }

  /**
   * Creates a Table with Qbeast
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
      tableCreationMode: TableCreationModes.CreationMode,
      tableFactory: IndexedTableFactory,
      existingSessionCatalog: SessionCatalog): Unit = {
    // These two keys are tableProperties in data source v2 but not in v1, so we have to filter
    // them out. Otherwise property consistency checks will fail.
    val tableProperties = allTableProperties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case TableCatalog.PROP_EXTERNAL => false
      case "path" => false
      case _ => true
    }

    val isPathTable = QbeastCatalogUtils.isPathTable(ident)

    if (isPathTable
      && allTableProperties.containsKey("location")
      // The location property can be qualified and different from the path in the identifier, so
      // we check `endsWith` here.
      && Option(allTableProperties.get("location")).exists(!_.endsWith(ident.name()))) {
      throw AnalysisExceptionFactory.create(
        s"CREATE TABLE contains two different locations: ${ident.name()} " +
          s"and ${allTableProperties.get("location")}.")
    }

    val location = if (isPathTable) {
      Option(ident.name())
    } else {
      Option(allTableProperties.get("location"))
    }
    val id = TableIdentifier(ident.name(), ident.namespace().lastOption)
    val locUriOpt = location.map(CatalogUtils.stringToURI)
    val existingTableOpt = QbeastCatalogUtils.getExistingTableIfExists(id, existingSessionCatalog)
    val loc = locUriOpt
      .orElse(existingTableOpt.flatMap(_.storage.locationUri))
      .getOrElse(existingSessionCatalog.defaultTablePath(id))

    val storage = DataSource
      .buildStorageFormatFromOptions(writeOptions)
      .copy(locationUri = Option(loc))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val commentOpt = Option(allTableProperties.get("comment"))
    val (partitionColumns, bucketSpec) = SparkTransformUtils.convertTransforms(partitions)

    val table = new CatalogTable(
      identifier = id,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some("qbeast"),
      partitionColumnNames = partitionColumns,
      bucketSpec = bucketSpec,
      properties = tableProperties.toMap,
      comment = commentOpt)

    val append = tableCreationMode.mode == SaveMode.Append
    dataFrame.map { df =>
      tableFactory
        .getIndexedTable(QTableID(loc.toString))
        .save(df, allTableProperties.asScala.toMap, append)
    }

    updateCatalog(tableCreationMode, table, isPathTable, existingTableOpt, existingSessionCatalog)
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
      operation: TableCreationModes.CreationMode,
      table: CatalogTable,
      isPathTable: Boolean,
      existingTableOpt: Option[CatalogTable],
      existingSessionCatalog: SessionCatalog): Unit = {

    operation match {
      case _ if isPathTable => // do nothing
      case TableCreationModes.Create =>
        existingSessionCatalog.createTable(
          table,
          ignoreIfExists = existingTableOpt.isDefined,
          validateLocation = false)
      case TableCreationModes.Replace | TableCreationModes.CreateOrReplace
          if existingTableOpt.isDefined =>
        existingSessionCatalog.alterTable(table)
      case TableCreationModes.Replace =>
        val ident = Identifier.of(table.identifier.database.toArray, table.identifier.table)
        throw new CannotReplaceMissingTableException(ident)
      case TableCreationModes.CreateOrReplace =>
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
    val schema = table.schema()

    table match {
      case V1TableQbeast(t) =>
        val catalogTable = t.v1Table

        val path: String = if (catalogTable.tableType == CatalogTableType.EXTERNAL) {
          // If it's an EXTERNAL TABLE, we can find the path through the Storage Properties
          catalogTable.storage.locationUri.get.toString
        } else if (catalogTable.tableType == CatalogTableType.MANAGED) {
          // If it's a MANAGED TABLE, the location is set in the former catalogTable
          catalogTable.location.toString
        } else {
          // Otherwise, TODO
          throw AnalysisExceptionFactory.create("No path found for table " + table.name())
        }
        new QbeastTableImpl(
          catalogTable.identifier.identifier,
          new Path(path),
          prop.asScala.toMap,
          Some(schema),
          Some(catalogTable),
          tableFactory)

      case DeltaTableV2(_, path, catalogTable, tableIdentifier, _, options, _) =>
        new QbeastTableImpl(
          tableIdentifier.get,
          path,
          options,
          Some(schema),
          catalogTable,
          tableFactory)

      case _ => table
    }
  }

}
