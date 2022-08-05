/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

class QbeastAnalysis(spark: SparkSession) extends Rule[LogicalPlan] {

  private def toV1Relation(
      dataSourceV2Relation: DataSourceV2Relation,
      table: QbeastTableImpl): LogicalRelation = {

    val underlyingRelation = table.toBaseRelation
    LogicalRelation(underlyingRelation, dataSourceV2Relation.output, None, isStreaming = false)

  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // This rule falls back to V1 nodes, since we don't have a V2 reader for Delta right now
    case v2Relation @ DataSourceV2Relation(t: QbeastTableImpl, _, _, _, _) =>
      toV1Relation(v2Relation, t)
  }

}
