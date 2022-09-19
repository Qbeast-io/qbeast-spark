/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.context.QbeastContext
import io.qbeast.spark.internal.QbeastOptions.checkQbeastProperties
import io.qbeast.spark.internal.sources.v2.QbeastStagedTableImpl
import org.apache.spark.sql.{SparkCatalogUtils, SparkSession}
import org.apache.spark.sql.connector.catalog.{
  CatalogExtension,
  CatalogPlugin,
  Identifier,
  NamespaceChange,
  StagedTable,
  StagingTableCatalog,
  SupportsNamespaces,
  Table,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * QbeastCatalog is a CatalogExtenssion with StagingTableCatalog
 */
class QbeastCatalog[T <: TableCatalog with SupportsNamespaces]
    extends CatalogExtension
    with SupportsNamespaces
    with StagingTableCatalog {

  private val tableFactory = QbeastContext.indexedTableFactory

  private var delegatedCatalog: CatalogPlugin = null

  private var catalogName: String = null

  private def getSessionCatalog(): T = {
    val sessionCatalog = delegatedCatalog match {
      case null =>
        // In this case, any catalog has been delegated, so we need to search for the default
        SparkCatalogUtils.getV2SessionCatalog(SparkSession.active)
      case o => o
    }

    sessionCatalog.asInstanceOf[T]
  }

  override def loadTable(ident: Identifier): Table = {
    val superTable = getSessionCatalog().loadTable(ident)
    if (QbeastCatalogUtils.isQbeastProvider(superTable.properties().asScala.get("provider"))) {
      QbeastCatalogUtils.loadQbeastTable(superTable, tableFactory)
    } else {
      superTable
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    if (QbeastCatalogUtils.isQbeastProvider(properties.asScala.get("provider"))) {
      checkQbeastProperties(properties.asScala.toMap)
      QbeastCatalogUtils.loadQbeastTable(
        getSessionCatalog().createTable(ident, schema, partitions, properties),
        tableFactory)
    } else {
      getSessionCatalog().createTable(ident, schema, partitions, properties)
    }

  }

  /**
   * For StageReplace, StageReplaceOrCreate and StageCreate, the following pipeline is executed
   * 1. Check if it's Qbeast Provider
   * 2. Create a QbeastStagedTable.
   *  This type of table allows to commit the changes atomically to the Catalog.
   * 3. If it was not a QbeastProvider, it delegates the creation/replacement to the DeltaCatalog
   */

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastProvider(properties.asScala.get("provider"))) {
      new QbeastStagedTableImpl(
        ident,
        schema,
        partitions,
        TableCreationModes.Replace,
        properties,
        tableFactory)
    } else {
      DefaultStagedTable(
        ident,
        getSessionCatalog().createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastProvider(properties.asScala.get("provider"))) {
      new QbeastStagedTableImpl(
        ident,
        schema,
        partitions,
        TableCreationModes.CreateOrReplace,
        properties,
        tableFactory)
    } else {
      DefaultStagedTable(
        ident,
        getSessionCatalog().createTable(ident, schema, partitions, properties),
        this)

    }
  }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastProvider(properties.asScala.get("provider"))) {
      new QbeastStagedTableImpl(
        ident,
        schema,
        partitions,
        TableCreationModes.Create,
        properties,
        tableFactory)
    } else {
      DefaultStagedTable(
        ident,
        getSessionCatalog().createTable(ident, schema, partitions, properties),
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

  override def dropNamespace(namespace: Array[String]): Boolean =
    getSessionCatalog().dropNamespace(namespace)

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize the catalog with the corresponding name
    this.catalogName = name
  }

  override def name(): String = catalogName

  override def setDelegateCatalog(delegate: CatalogPlugin): Unit = {
    // Check if the delegating catalog has Table and SupportsNamespace properties
    if (delegate.isInstanceOf[TableCatalog] && delegate.isInstanceOf[SupportsNamespaces]) {
      this.delegatedCatalog = delegate
    } else throw new IllegalArgumentException("Invalid session catalog: " + delegate)
  }

}
