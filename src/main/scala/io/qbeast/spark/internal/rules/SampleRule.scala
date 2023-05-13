/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.expressions.QbeastSample
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sample}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import io.qbeast.spark.delta.OTreeIndex

/**
 * Rule object that transforms a Sample operator over a QbeastRelation
 * into a suitable Filter for Qbeast
 */
object SampleRule extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown { case Sample(l, u, false, _, child) =>
      child match {
        case QbeastRelation(r) => Filter(QbeastSample(l, u), r)

        case Project(_, Filter(_, QbeastRelation(r))) => Filter(QbeastSample(l, u), r)

        case Filter(_, QbeastRelation(r)) => Filter(QbeastSample(l, u), r)

        case Project(_, QbeastRelation(r)) => Filter(QbeastSample(l, u), r)

        case _ => plan
      }
    }
  }

}

/**
 * QbeastRelation matching pattern
 */
object QbeastRelation {

  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case r @ LogicalRelation(HadoopFsRelation(_: OTreeIndex, _, _, _, _, _), _, _, _) =>
      Some(r)
    case _ => None
  }

}
