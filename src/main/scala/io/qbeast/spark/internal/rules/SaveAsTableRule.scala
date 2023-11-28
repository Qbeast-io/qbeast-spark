/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.sources.catalog.QbeastCatalogUtils.isQbeastProvider
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateTableAsSelect,
  LogicalPlan,
  ReplaceTableAsSelect
}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rule class that enforces to pass all the write options to the Table Implementation
 * @param spark
 *   the SparkSession
 */
class SaveAsTableRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // When CreateTableAsSelect statement is in place for qbeast
    // We need to pass the writeOptions as properties to the creation of the table
    // to make sure columnsToIndex is present
    plan transformDown {
      case saveAsSelect: CreateTableAsSelect if isQbeastProvider(saveAsSelect.tableSpec) =>
        val finalProperties = saveAsSelect.writeOptions ++ saveAsSelect.tableSpec.properties
        saveAsSelect.copy(tableSpec = saveAsSelect.tableSpec.copy(properties = finalProperties))
      case replaceAsSelect: ReplaceTableAsSelect if isQbeastProvider(replaceAsSelect.tableSpec) =>
        val finalProperties = replaceAsSelect.tableSpec.properties ++ replaceAsSelect.writeOptions
        replaceAsSelect.copy(tableSpec =
          replaceAsSelect.tableSpec.copy(properties = finalProperties))
    }
  }

}
