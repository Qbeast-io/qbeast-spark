/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.delta.OTreeIndex
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.{DeltaTable, DeltaTableUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Rule class that changes the file index of the DeltaRelation
 * underneath the QbeastRelation for an OTreeIndex
 * @param spark The SparkSession to extend
 */
class ReplaceFileIndex(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case r @ LogicalRelation(QbeastBaseRelation(delta, _), _, _, _) =>
      r.copy(relation = delta) match {
        case p @ DeltaTable(fileIndex: TahoeLogFileIndex) =>
          DeltaTableUtils.replaceFileIndex(p, OTreeIndex(fileIndex))
      }
  }

}
