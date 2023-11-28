/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.context.QbeastContext
import io.qbeast.spark.internal.QbeastOptions.checkQbeastProperties
import io.qbeast.spark.internal.sources.v2.{QbeastStagedTableImpl, QbeastTableImpl}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{
  NoSuchDatabaseException,
  NoSuchNamespaceException,
  NoSuchTableException
}
import org.apache.spark.sql.{SparkCatalogUtils, SparkSession}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * QbeastCatalog is a CatalogExtenssion that supports Namespaces and the CREATION and/or
 * REPLACEMENT of tables QbeastCatalog uses a session catalog of type T to delegate high-level
 * operations
 */
class QbeastCatalog[T <: TableCatalog with SupportsNamespaces with FunctionCatalog]
    extends CatalogExtension
    with SupportsNamespaces
    with StagingTableCatalog {

  private lazy val spark = SparkSession.active

  private val tableFactory = QbeastContext.indexedTableFactory

  private val deltaCatalog: DeltaCatalog = new DeltaCatalog()

  private var delegatedCatalog: CatalogPlugin = null

  private var catalogName: String = null

  /**
   * Gets the delegated catalog of the session
   * @return
   */
  private def getDelegatedCatalog(): T = {
    val sessionCatalog = delegatedCatalog match {
      case null =>
        // In this case, any catalog has been delegated, so we need to search for the default
        SparkCatalogUtils.getV2SessionCatalog(SparkSession.active)
      case o => o
    }
    sessionCatalog.asInstanceOf[T]
  }

  /**
   * Gets the session catalog depending on provider properties, if any
   *
   * The intention is to include the different catalog providers while we add the integrations
   * with the formats. For example, for "delta" provider it will return a DeltaCatalog instance.
   *
   * In this way, users may only need to instantiate one single unified catalog.
   * @param properties
   *   the properties with the provider parameter
   * @return
   */
  private def getSessionCatalog(properties: Map[String, String] = Map.empty): T = {
    properties.get("provider") match {
      case Some("delta") => deltaCatalog.asInstanceOf[T]
      case _ => getDelegatedCatalog()
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      getSessionCatalog().loadTable(ident) match {
        case table
            if QbeastCatalogUtils.isQbeastProvider(table.properties().asScala.get("provider")) =>
          QbeastCatalogUtils.loadQbeastTable(table, tableFactory)
        case o => o
      }
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException
          if QbeastCatalogUtils.isPathTable(ident) =>
        new QbeastTableImpl(
          TableIdentifier(ident.name(), ident.namespace().headOption),
          new Path(ident.name()),
          Map.empty,
          tableFactory = tableFactory)
    }
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    createTable(ident, SparkCatalogV2Util.v2ColumnsToStructType(columns), partitions, properties)

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    if (QbeastCatalogUtils.isQbeastProvider(properties)) {
      checkQbeastProperties(properties.asScala.toMap)
      // Create the table
      QbeastCatalogUtils.createQbeastTable(
        ident,
        schema,
        partitions,
        properties,
        Map.empty,
        dataFrame = None,
        TableCreationMode.CREATE_TABLE,
        tableFactory,
        spark.sessionState.catalog)
      // Load the table
      loadTable(ident)
    } else {
      getSessionCatalog(properties.asScala.toMap).createTable(
        ident,
        SparkCatalogV2Util.structTypeToV2Columns(schema),
        partitions,
        properties)
    }

  }

  /**
   * For StageReplace, StageReplaceOrCreate and StageCreate, the following pipeline is executed:
   *   1. Check if it's a Qbeast Provider 2. If true, it creates a QbeastStagedTable, which allows
   *      atomizing the changes to the Catalog. 3. Otherwise, output a DefaultStagedTable
   */

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastProvider(properties)) {
      new QbeastStagedTableImpl(
        ident,
        schema,
        partitions,
        TableCreationMode.REPLACE_TABLE,
        properties,
        tableFactory)
    } else {
      val sessionCatalog = getSessionCatalog(properties.asScala.toMap)
      if (sessionCatalog.tableExists(ident)) {
        sessionCatalog.dropTable(ident)
      }
      DefaultStagedTable(
        ident,
        sessionCatalog.createTable(
          ident,
          SparkCatalogV2Util.structTypeToV2Columns(schema),
          partitions,
          properties),
        this)
    }
  }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastProvider(properties)) {
      new QbeastStagedTableImpl(
        ident,
        schema,
        partitions,
        TableCreationMode.CREATE_OR_REPLACE,
        properties,
        tableFactory)
    } else {
      val sessionCatalog = getSessionCatalog(properties.asScala.toMap)
      if (sessionCatalog.tableExists(ident)) {
        sessionCatalog.dropTable(ident)
      }
      DefaultStagedTable(
        ident,
        sessionCatalog.createTable(
          ident,
          SparkCatalogV2Util.structTypeToV2Columns(schema),
          partitions,
          properties),
        this)

    }
  }

  override def stageCreate(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {

    stageCreate(ident, SparkCatalogV2Util.v2ColumnsToStructType(columns), partitions, properties)

  }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastProvider(properties)) {
      new QbeastStagedTableImpl(
        ident,
        schema,
        partitions,
        TableCreationMode.CREATE_TABLE,
        properties,
        tableFactory)
    } else {
      DefaultStagedTable(
        ident,
        getSessionCatalog(properties.asScala.toMap)
          .createTable(
            ident,
            SparkCatalogV2Util.structTypeToV2Columns(schema),
            partitions,
            properties),
        this)
    }
  }

  override def listTables(namespace: Array[String]): Array[Identifier] =
    getSessionCatalog().listTables(namespace)

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    getSessionCatalog().alterTable(ident, changes.head)

  override def dropTable(ident: Identifier): Boolean = getSessionCatalog().dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    getSessionCatalog().renameTable(oldIdent, newIdent)

  override def listNamespaces(): Array[Array[String]] = getSessionCatalog().listNamespaces()

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    getSessionCatalog().listNamespaces(namespace)

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    getSessionCatalog().loadNamespaceMetadata(namespace)

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit =
    getSessionCatalog().createNamespace(namespace, metadata)

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    getSessionCatalog().alterNamespace(namespace, changes.head)

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean =
    getSessionCatalog().dropNamespace(namespace, cascade)

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize the catalog with the corresponding name
    this.catalogName = name
    // Initialize the catalog in any other provider that we can integrate with
    this.deltaCatalog.initialize(name, options)
  }

  override def name(): String = catalogName

  override def setDelegateCatalog(delegate: CatalogPlugin): Unit = {
    // Check if the delegating catalog has Table and SupportsNamespace properties
    if (delegate.isInstanceOf[TableCatalog] && delegate.isInstanceOf[SupportsNamespaces]) {
      this.delegatedCatalog = delegate
      // Set delegated catalog in any other provider that we can integrate with
      this.deltaCatalog.setDelegateCatalog(delegate)
    } else throw new IllegalArgumentException("Invalid session catalog: " + delegate)
  }

  override def listFunctions(namespace: Array[String]): Array[Identifier] =
    getSessionCatalog().listFunctions(namespace)

  override def loadFunction(ident: Identifier): UnboundFunction =
    getSessionCatalog().loadFunction(ident)

}
