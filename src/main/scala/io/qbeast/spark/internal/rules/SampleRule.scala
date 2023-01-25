/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.core.model.{Weight, WeightRange}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sample}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import io.qbeast.spark.delta.OTreeIndex
import io.qbeast.spark.internal.expressions.{QbeastMurmur3Hash}
import org.apache.spark.sql.catalyst.expressions.{And, GreaterThanOrEqual, LessThan, Literal}

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
   * Transforms the Sample Operator to a Filter
   *
   * @param sample          the Sample Operator
   * @param logicalRelation the LogicalRelation underneath
   * @param indexedColumns  the IndexedColumns of the LogicalRelation
   * @return the new Filter
   */
  private def transformSampleToFilter(
      sample: Sample,
      logicalRelation: LogicalRelation): Filter = {

    val weightRange = extractWeightRange(sample)
    val qbeastHash = new QbeastMurmur3Hash(Seq.empty) // TODO Change that HASH

    Filter(
      And(
        LessThan(qbeastHash, Literal(weightRange.to.value)),
        GreaterThanOrEqual(qbeastHash, Literal(weightRange.from.value))),
      sample.child)

  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown { case s @ Sample(_, _, false, _, child) =>
      child match {
        case QbeastRelation(l) => transformSampleToFilter(s, l)

        case Project(_, Filter(_, QbeastRelation(l))) =>
          transformSampleToFilter(s, l)

        case Filter(_, QbeastRelation(l)) =>
          transformSampleToFilter(s, l)

        case Project(_, QbeastRelation(l)) =>
          transformSampleToFilter(s, l)

        case _ => s
      }

    }
  }

}

/**
 * QbeastRelation matching pattern
 */
object QbeastRelation {

  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {

    case l @ LogicalRelation(HadoopFsRelation(o: OTreeIndex, _, _, _, _, _), _, _, _) =>
      Some(l)
    case _ => None
  }

}
