/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.unsafe.types.UTF8String

private[query] trait QueryFiltersUtils {

  lazy val spark: SparkSession = SparkSession.active
  lazy val nameEquality: Resolver = spark.sessionState.analyzer.resolver

  def hasQbeastColumnReference(expr: Expression, indexedColumns: Seq[String]): Boolean = {
    expr.references.forall { r =>
      indexedColumns.exists(nameEquality(r.name, _))
    }
  }

  def isQbeastWeightExpression(expression: Expression): Boolean = {
    expression match {
      case BinaryComparison(_: QbeastMurmur3Hash, _) => true
      case _ => false
    }
  }

  def isDisjunctivePredicate(condition: Expression): Boolean = {
    condition.isInstanceOf[Or]
  }

  def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  def hasColumnReference(expr: Expression, columnName: String): Boolean = {
    expr.references.forall(r => nameEquality(r.name, columnName))
  }

  def sparkTypeToCoreType(value: Any): Any = {
    value match {
      case s: UTF8String => s.toString
      case _ => value
    }
  }

  // Based on Delta DataSkippingReader
  // Since we already know this filter are eligible for skipping
  // we directly output the RangePredicate
  def inToDataFilter(column: Expression, values: Seq[Any]): Seq[Expression] = {

    val dataType = column.dataType
    lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)
    val min = Literal(values.min(ordering), dataType)
    val max = Literal(values.max(ordering), dataType)
    Seq(LessThanOrEqual(column, max), GreaterThanOrEqual(column, min))

  }

  def transformInPredicates(expression: Expression): Seq[Expression] = {
    expression match {
      case in @ In(a, values) if in.inSetConvertible =>
        inToDataFilter(a, values.map(_.asInstanceOf[Literal].value))

      // The optimizer automatically converts all but the shortest eligible IN-lists to InSet.
      case InSet(a, values) =>
        inToDataFilter(a, values.toSeq)

      // Treat IN(... subquery ...) as a normal IN-list, since the subquery already ran before now.
      case in: InSubqueryExec =>
        // At this point the subquery has been materialized so it is safe to call get on the Option.
        inToDataFilter(in.child, in.values().get.toSeq)

      // If the Filter involves any other predicates, return without pre-processing
      case other => other :: Nil
    }
  }

}
