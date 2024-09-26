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

import io.qbeast.spark.internal.rules.QbeastAnalysisUtils._
import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.SparkSession

/**
 * Analyzes and resolves the Spark Plan before Optimization
 * @param spark
 *   the SparkSession
 */
class QbeastAnalysis(spark: SparkSession) extends Rule[LogicalPlan] {

  /**
   * Returns the V1Relation from a V2Relation
   * @param dataSourceV2Relation
   *   the V2Relation
   * @param table
   *   the underlying table
   * @return
   *   the LogicalRelation
   */
  private def toV1Relation(
      dataSourceV2Relation: DataSourceV2Relation,
      table: QbeastTableImpl): LogicalRelation = {

    val underlyingRelation = table.toBaseRelation
    LogicalRelation(underlyingRelation, dataSourceV2Relation.output, None, isStreaming = false)

  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // This rule is a hack to return a V1 relation for reading
    // Because we didn't implemented SupportsRead on QbeastTableImpl yet
    case v2Relation @ DataSourceV2Relation(t: QbeastTableImpl, _, _, _, _) =>
      toV1Relation(v2Relation, t)

    // Appending data by Ordinal (no schema provided) INSERT INTO tbl VALUES(...)
    case appendData @ AppendQbeastTable(relation, table)
        if !appendData.isByName && needSchemaAdjustment(
          table.name(),
          appendData.query,
          relation.schema) =>
      val projection =
        resolveQueryColumnsByOrdinal(appendData.query, relation.output, table.name())
      if (projection != appendData.query) {
        appendData.copy(query = projection)
      } else {
        appendData
      }
  }

}

/**
 * Object for detecting AppendData into QbeastTable pattern
 */
object AppendQbeastTable {

  def unapply(a: AppendData): Option[(DataSourceV2Relation, QbeastTableImpl)] = {
    if (a.query.resolved) {
      a.table match {
        case r: DataSourceV2Relation if r.table.isInstanceOf[QbeastTableImpl] =>
          Some((r, r.table.asInstanceOf[QbeastTableImpl]))
        case _ => None
      }
    } else {
      None
    }
  }

}
