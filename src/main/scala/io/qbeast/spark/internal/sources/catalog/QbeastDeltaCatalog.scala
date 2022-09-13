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

class QbeastDeltaCatalog extends DeltaCatalog {

  private val tableFactory = QbeastContext.indexedTableFactory

  override def loadTable(ident: Identifier): Table = {
    val superTable = super.loadTable(ident)
    if (QbeastCatalogUtils.isQbeastTable(superTable.properties().asScala.get("provider"))) {
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
    if (QbeastCatalogUtils.isQbeastTable(superTable.properties().asScala.get("provider"))) {
      checkQbeastProperties(properties.asScala.toMap)
      QbeastCatalogUtils.loadQbeastTable(superTable, tableFactory)
    } else {
      super.createTable(ident, schema, partitions, properties)
    }

  }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = {
    if (QbeastCatalogUtils.isQbeastTable(properties.asScala.get("provider"))) {
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
    if (QbeastCatalogUtils.isQbeastTable(properties.asScala.get("provider"))) {
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
    if (QbeastCatalogUtils.isQbeastTable(properties.asScala.get("provider"))) {
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
