/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.expressions.QbeastSample
import io.qbeast.spark.spark.delta.OTreeFileIndex
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sample
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Rule object that transforms a Sample operator over a plan based on  Qbeast
 * relation into a suitable Filter for Qbeast
 */
object SampleRule extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case Sample(l, u, false, _, child) if (isBasedOnQbeastRelation(child)) =>
      Filter(QbeastSample(l, u), child)
  }

  private def isBasedOnQbeastRelation(plan: LogicalPlan): Boolean = plan match {
    case LogicalRelation(HadoopFsRelation(_: OTreeFileIndex, _, _, _, _, _), _, _, _) => true
    case Project(_, child) => isBasedOnQbeastRelation(child)
    case Filter(_, child) => isBasedOnQbeastRelation(child)
    case _ => false
  }

}
