/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.context.QbeastContext
import io.qbeast.spark.internal.QbeastOptions.checkQbeastProperties
import io.qbeast.spark.internal.sources.v2.QbeastStagedTableImpl
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters._

/**
 * QbeastDeltaCatalog is a DelegatingCatalogExtension with StagingTableCatalog
 * that extends the current implementation of DeltaCatalog.
 * This would allow to populate Delta Tables with this implementation,
 * along with the creation of Qbeast tables
 */
class QbeastDeltaCatalog extends DeltaCatalog {

  private val tableFactory = QbeastContext.indexedTableFactory

  override def loadTable(ident: Identifier): Table = {
    val superTable = super.loadTable(ident)
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

    val superTable = super.createTable(ident, schema, partitions, properties)
    if (QbeastCatalogUtils.isQbeastProvider(superTable.properties().asScala.get("provider"))) {
      checkQbeastProperties(properties.asScala.toMap)
      QbeastCatalogUtils.loadQbeastTable(superTable, tableFactory)
    } else {
      super.createTable(ident, schema, partitions, properties)
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
      super.stageReplace(ident, schema, partitions, properties)
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
      super.stageCreateOrReplace(ident, schema, partitions, properties)

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
      super.stageCreate(ident, schema, partitions, properties)
    }
  }

}
