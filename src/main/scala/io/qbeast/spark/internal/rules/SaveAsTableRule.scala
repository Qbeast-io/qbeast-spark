/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.sources.catalog.QbeastCatalogUtils.isQbeastProvider
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect, TableSpec, TableSpecBase}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.SparkSession

/**
 * Rule class that enforces to pass all the write options to the Table Implementation
 * @param spark
 *   the SparkSession
 */
class SaveAsTableRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  private def createTableSpec(tableSpec: TableSpecBase, writeOptions: Map[String, String]): TableSpec = {
    val finalProperties = writeOptions ++ tableSpec.properties
    TableSpec(
      properties = finalProperties,
      provider = tableSpec.provider,
      options = writeOptions,
      location = tableSpec.location,
      comment = tableSpec.comment,
      serde = tableSpec.serde,
      external = tableSpec.external)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // When CreateTableAsSelect statement is in place for qbeast
    // We need to pass the writeOptions as properties to the creation of the table
    // to make sure columnsToIndex is present
    plan transformDown {
      case saveAsSelect: CreateTableAsSelect if isQbeastProvider(saveAsSelect.tableSpec.properties) =>
        val tableSpec = saveAsSelect.tableSpec
        val writeOptions = saveAsSelect.writeOptions
       saveAsSelect.copy(tableSpec = createTableSpec(tableSpec, writeOptions))
      case replaceAsSelect: ReplaceTableAsSelect if isQbeastProvider(replaceAsSelect.tableSpec.properties) =>
        val tableSpec = replaceAsSelect.tableSpec
        val writeOptions = replaceAsSelect.writeOptions
        replaceAsSelect.copy(tableSpec = createTableSpec(tableSpec, writeOptions))
    }
  }

}
