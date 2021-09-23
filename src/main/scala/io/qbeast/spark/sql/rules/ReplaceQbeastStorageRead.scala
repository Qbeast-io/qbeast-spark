/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.rules

import io.qbeast.spark.context.QbeastContext
import io.qbeast.spark.sql.files.OTreeIndex
import io.qbeast.spark.sql.sources.QbeastBaseRelation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.{DeltaTable, DeltaTableUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Rule class that changes the file index of the DeltaRelation
 * underneath the QbeastRelation
 * for an OTreeIndex
 * @param spark The SparkSession to extend
 */
class ReplaceQbeastStorageRead(spark: SparkSession) extends Rule[LogicalPlan] {

  private val desiredCubeSize: Int = QbeastContext.oTreeAlgorithm.desiredCubeSize

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case r @ LogicalRelation(QbeastBaseRelation(delta, _), _, _, _) =>
      r.copy(relation = delta) match {
        case p @ DeltaTable(fileIndex: TahoeLogFileIndex) =>
          DeltaTableUtils.replaceFileIndex(p, OTreeIndex(fileIndex, desiredCubeSize))
      }
  }

}
