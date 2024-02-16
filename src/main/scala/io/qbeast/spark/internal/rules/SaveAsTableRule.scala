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
 * @param spark the SparkSession
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
