/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.core.model.Weight
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, GreaterThanOrEqual, LessThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sample}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.SparkSession
import io.qbeast.core.model.WeightRange

/**
 * Rule class that transforms a Sample operator over a QbeastRelation
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
  private def transformSampleToFilter(
      sample: Sample,
      logicalRelation: LogicalRelation,
      qbeastBaseRelation: QbeastBaseRelation): LogicalPlan = {

    val weightRange = extractWeightRange(sample)

    val columns =
      qbeastBaseRelation.columnTransformers.map(c =>
        logicalRelation.output.find(_.name == c.columnName).get)
    val qbeastHash = new QbeastMurmur3Hash(columns)
    val filter = And(
      LessThan(qbeastHash, Literal(weightRange.to.value)),
      GreaterThanOrEqual(qbeastHash, Literal(weightRange.from.value)))
    Filter(filter, logicalRelation)

  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {

      case s @ Sample(_, _, false, _, QbeastRelation(l, q)) => transformSampleToFilter(s, l, q)

      case s @ Sample(_, _, false, _, child) =>
        child match {
          case Project(projectList, Filter(condition, QbeastRelation(l, q))) =>
            Project(projectList, Filter(condition, transformSampleToFilter(s, l, q)))
          case Filter(condition, QbeastRelation(l, q)) =>
            Filter(condition, transformSampleToFilter(s, l, q))
          case Project(projectList, QbeastRelation(l, q)) =>
            Project(projectList, transformSampleToFilter(s, l, q))
          case _ => s
        }

    }
  }

}

/**
 * QbeastRelation matching pattern
 */
object QbeastRelation {

  def unapply(plan: LogicalPlan): Option[(LogicalRelation, QbeastBaseRelation)] = plan match {
    case l @ LogicalRelation(q: QbeastBaseRelation, _, _, _) => Some((l, q))
    case _ => None
  }

}
