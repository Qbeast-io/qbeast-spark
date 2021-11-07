/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import io.qbeast.model.{Revision, Weight}
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{
  And,
  Expression,
  GreaterThanOrEqual,
  HashExpression,
  LessThan,
  Literal,
  Murmur3HashFunction,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sample}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Rule class that transforms a Sample operator over a Qbeast Relation
 * into a suitable Filter for Qbeast
 * @param spark The current SparkSession
 */
class SampleRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  def transformSampleToFilter(
      sample: Sample,
      logicalRelation: LogicalRelation,
      revision: Revision): Expression = {
    val minWeight = Weight(sample.lowerBound).value
    val maxWeight = Weight(sample.upperBound).value

    val columns = revision.columnTransformers.map(c =>
      logicalRelation.output.find(_.name == c.columnName).get)
    val weight = new QbeastMurmur3Hash(columns)
    And(LessThan(weight, Literal(maxWeight)), GreaterThanOrEqual(weight, Literal(minWeight)))

  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {

      case s @ Sample(
            _,
            _,
            false,
            _,
            l @ LogicalRelation(QbeastBaseRelation(_, revision), _, _, _)) =>
        Filter(transformSampleToFilter(s, l, revision), l)

      case s @ Sample(_, _, false, _, child) =>
        child match {
          case ProjectionFilter(
                l @ LogicalRelation(QbeastBaseRelation(_, columnsToIndex), _, _, _),
                projectList) =>
            Project(projectList, Filter(transformSampleToFilter(s, l, columnsToIndex), child))
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

object Functions {

  def qbeastHash(cols: Column*): Column =
    new Column(new QbeastMurmur3Hash(cols.map(_.expr)))

}

/**
 * Qbeast hash expression based on Murmur3Hash algorithm
 * @param children Sequence of expressions to hash
 * @param seed Seed for the Hash Expression
 */
case class QbeastMurmur3Hash(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  def this(arguments: Seq[Expression]) = this(arguments, 42)

  override def dataType: DataType = IntegerType

  override def prettyName: String = "qbeast_hash"

  override protected def hasherClassName: String = classOf[Murmur3_x86_32].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    Murmur3HashFunction.hash(value, dataType, seed).toInt
  }

}
