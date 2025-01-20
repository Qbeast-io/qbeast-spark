package io.qbeast.spark.utils

import org.apache.spark.sql.catalyst.expressions.Exists
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.InSubquery
import org.apache.spark.sql.catalyst.expressions.LateralSubquery
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object SubqueryExpression {

  def unapply(expr: Expression): Option[LogicalPlan] = expr match {
    case subquery: ScalarSubquery => Some(subquery.plan)
    case exists: Exists => Some(exists.plan)
    case subquery: InSubquery => Some(subquery.query.plan)
    case subquery: LateralSubquery => Some(subquery.plan)
    case _ => None
  }

}
