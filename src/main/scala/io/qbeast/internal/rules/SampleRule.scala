/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.internal.rules

import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.Weight
import io.qbeast.core.model.WeightRange
import io.qbeast.spark.index.DefaultFileIndex
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import io.qbeast.IndexedColumns
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sample
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.SparkSession

/**
 * Rule class that transforms a Sample operator over a QbeastRelation into a suitable Filter for
 * Qbeast
 * @param spark
 *   The current SparkSession
 */
class SampleRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  /**
   * Extracts the weight range of the Sample operator
   * @param sample
   *   the Sample operator
   * @return
   *   the Range of Weights
   */
  private def extractWeightRange(sample: Sample): WeightRange = {
    val minWeight = Weight(sample.lowerBound)
    val maxWeight = Weight(sample.upperBound)
    WeightRange(minWeight, maxWeight)
  }

  /**
   * Transforms the Sample Operator to a Filter
   * @param sample
   *   the Sample Operator
   * @param logicalRelation
   *   the LogicalRelation underneath
   * @param indexedColumns
   *   the IndexedColumns of the LogicalRelation
   * @return
   *   the new Filter
   */
  private def transformSampleToFilter(
      sample: Sample,
      logicalRelation: LogicalRelation,
      indexedColumns: IndexedColumns): Filter = {

    val weightRange = extractWeightRange(sample)

    val columns =
      indexedColumns.map(c => logicalRelation.output.find(_.name == c).get)
    val qbeastHash = new QbeastMurmur3Hash(columns)

    Filter(
      And(
        LessThan(qbeastHash, Literal(weightRange.to.value)),
        GreaterThanOrEqual(qbeastHash, Literal(weightRange.from.value))),
      sample.child)

  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown { case s @ Sample(_, _, false, _, child) =>
      child match {
        case QbeastRelation(l, q) => transformSampleToFilter(s, l, q)

        case Project(_, Filter(_, QbeastRelation(l, q))) =>
          transformSampleToFilter(s, l, q)

        case Filter(_, QbeastRelation(l, q)) =>
          transformSampleToFilter(s, l, q)

        case Project(_, QbeastRelation(l, q)) =>
          transformSampleToFilter(s, l, q)

        case _ => s
      }

    }
  }

}

/**
 * QbeastRelation matching pattern
 */
object QbeastRelation {

  def unapply(plan: LogicalPlan): Option[(LogicalRelation, IndexedColumns)] = plan match {

    case l @ LogicalRelation(
          q @ HadoopFsRelation(o: DefaultFileIndex, _, _, _, _, parameters),
          _,
          _,
          _) =>
      val qbeastOptions = QbeastOptions(parameters)
      val columnsToIndex = qbeastOptions.columnsToIndexParsed.map(_.columnName)
      Some((l, columnsToIndex))
    case _ => None
  }

}
