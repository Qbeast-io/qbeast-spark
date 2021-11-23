/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.model.Weight
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{
  And,
  GreaterThanOrEqual,
  LessThan,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sample}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.SparkSession
import io.qbeast.model.WeightRange

/**
 * Rule class that transforms a Sample operator over a Qbeast Relation
 * into a suitable Filter for Qbeast
 * @param spark The current SparkSession
 */
class SampleRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  /**
   * Extracts the weight range of the Sample operator
   * @param sample the Sample operator
   * @return the Range of Weights
   */
  private def extractWeightRange(sample: Sample): WeightRange = {
    val minWeight = Weight(sample.lowerBound)
    val maxWeight = Weight(sample.upperBound)
    WeightRange(minWeight, maxWeight)
  }

  /**
   * Transforms the Sample Operator to a Filter if suited
   * @param sample the Sample Operator
   * @param logicalRelation the relation underneath
   * @return the new LogicalPlan containing the Filter
   */
  def transformSampleToFilter(sample: Sample, logicalRelation: LogicalRelation): LogicalPlan = {

    logicalRelation match {
      case LogicalRelation(q: QbeastBaseRelation, output, _, _) =>
        val weightRange = extractWeightRange(sample)

        val columns =
          q.columnTransformers.map(c => output.find(_.name == c.columnName).get)
        val qbeastHash = new QbeastMurmur3Hash(columns)
        val filter = And(
          LessThan(qbeastHash, Literal(weightRange.to.value)),
          GreaterThanOrEqual(qbeastHash, Literal(weightRange.from.value)))
        Filter(filter, logicalRelation)

      case _ => sample

    }

  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {

      case s @ Sample(_, _, false, _, l: LogicalRelation) => transformSampleToFilter(s, l)

      case s @ Sample(_, _, false, _, child) =>
        child match {
          case ProjectionFilter(l: LogicalRelation, projectList) =>
            Project(projectList, transformSampleToFilter(s, l))
          case _ => s
        }

    }
  }

}

/**
 * Qbeast Projection Filter combination
 */
object ProjectionFilter {

  def unapply(plan: LogicalPlan): Option[(LogicalRelation, Seq[NamedExpression])] = plan match {
    case Project(projectList, Filter(_, l: LogicalRelation)) => Some((l, projectList))
    case Filter(_, l: LogicalRelation) => Some((l, l.output))
    case Project(projectList, l: LogicalRelation) => Some((l, projectList))
    case _ => None
  }

}
