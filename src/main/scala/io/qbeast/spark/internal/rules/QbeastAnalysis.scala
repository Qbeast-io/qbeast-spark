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

/**
 * Analyzes and resolves the Spark Plan before Optimization
 * @param spark the SparkSession
 */
class QbeastAnalysis(spark: SparkSession) extends Rule[LogicalPlan] {

  /**
   * Returns the V1Relation from a V2Relation
   * @param dataSourceV2Relation the V2Relation
   * @param table the underlying table
   * @return the LogicalRelation
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
  }

}
