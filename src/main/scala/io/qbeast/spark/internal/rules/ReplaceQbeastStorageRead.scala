/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Rule class that unwraps the BaseRelation
 * underneath the QbeastRelation
 * @param spark The SparkSession to extend
 */
class ReplaceQbeastStorageRead(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case r @ LogicalRelation(q: QbeastBaseRelation, _, _, _) =>
      r.copy(relation = q.relation)
  }

}
