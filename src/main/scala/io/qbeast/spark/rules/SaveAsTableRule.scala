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
package io.qbeast.spark.rules

import io.qbeast.spark.sources.catalog.QbeastCatalogUtils.isQbeastProvider
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.catalyst.plans.logical.TableSpecBase
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.SparkSession

/**
 * Rule class that enforces to pass all the write options to the Table Implementation
 * @param spark
 *   the SparkSession
 */
class SaveAsTableRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  private def createTableSpec(
      tableSpec: TableSpecBase,
      writeOptions: Map[String, String]): TableSpec = {
    val options = tableSpec match {
      case s: TableSpec => writeOptions ++ s.options
      case _ => writeOptions
    }
    TableSpec(
      properties = tableSpec.properties,
      provider = tableSpec.provider,
      options = options,
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
      case saveAsSelect: CreateTableAsSelect
          if isQbeastProvider(saveAsSelect.tableSpec.provider) =>
        val tableSpec = saveAsSelect.tableSpec
        val writeOptions = saveAsSelect.writeOptions
        saveAsSelect.copy(tableSpec = createTableSpec(tableSpec, writeOptions))
      case replaceAsSelect: ReplaceTableAsSelect
          if isQbeastProvider(replaceAsSelect.tableSpec.provider) =>
        val tableSpec = replaceAsSelect.tableSpec
        val writeOptions = replaceAsSelect.writeOptions
        replaceAsSelect.copy(tableSpec = createTableSpec(tableSpec, writeOptions))
    }
  }

}
