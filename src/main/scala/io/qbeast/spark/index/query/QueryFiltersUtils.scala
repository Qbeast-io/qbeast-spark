/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.transform.{HashTransformer, Transformer}
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

  /**
   * Checks if an expression references to a certain column
   * @param expr the Expression
   * @param columnName the name of the column
   * @return
   */
  def hasColumnReference(expr: Expression, columnName: String): Boolean = {
    expr.references.forall(r => nameEquality(r.name, columnName))
  }

  /**
   * Checks if an expression references to any of the Qbeast Indexed Columns
   * @param expr the Expression
   * @param indexedColumns the current indexed columns
   * @return
   */
  def hasQbeastColumnReference(expr: Expression, indexedColumns: Seq[String]): Boolean = {
    expr.references.forall { r =>
      indexedColumns.exists(nameEquality(r.name, _))
    }
  }

  /**
   * Checks if an Expression is a Qbeast Weight filter
   * @param expression the Expression
   * @return
   */

  def isQbeastWeightExpression(expression: Expression): Boolean = {
    expression match {
      case BinaryComparison(_: QbeastMurmur3Hash, _) => true
      case _ => false
    }
  }

  /**
   * Checks if an Expression is Disjunctive (OR)
   * @param condition the expression
   * @return
   */
  def isDisjunctiveExpression(condition: Expression): Boolean = {
    condition.isInstanceOf[Or]
  }

  /**
   * Recursively split Disjunctive operators (AND) in an expression
   *
   * @param condition the expression to evaluate
   * @return
   */

  def splitDisjunctiveExpressions(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctiveExpressions(cond1) ++ splitDisjunctiveExpressions(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Recursively split Conjunctive operators (OR) in an expression
   *
   * @param condition the expression to evaluate
   * @return
   */

  def splitConjunctiveExpressions(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctiveExpressions(cond1) ++ splitConjunctiveExpressions(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Convert an Spark String type to a Scala core type
   * @param value the value to convert
   * @return
   */

  def sparkTypeToCoreType(value: Any): Any = {
    value match {
      case s: UTF8String => s.toString
      case _ => value
    }
  }

  /**
   * Check if a column is eligible to filter by range
   * @param transformation the column transformation
   * @return true if is eligible
   */
  def isEligibleColumn(transformer: Transformer): Boolean = {
    transformer match {
      case _: HashTransformer => false
      case _ => true
    }
  }

  /**
   * Transform an IN expression into Range Expressions (GreaterThanOrEqual, LessThanOrEqual)
   * Based on Delta DataSkippingReader
   *
   * WARNING: expression that references columns with HashTransformation
   * would not be converted to a Range Expression
   * This is because HashTransformation does not have the properties
   * to order lexicographic strings
   *
   * @param column the column involved in the IN predicate
   * @param columnTransformation the transformation of the particular column
   * @param values the values in the set of IN predicates
   * @return
   */
  def inToRangeExpressions(
      column: Expression,
      columnTransformer: Transformer,
      values: Seq[Any]): Seq[Expression] = {

    if (isEligibleColumn(columnTransformer)) {
      val dataType = column.dataType
      lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)
      val min = Literal(values.min(ordering), dataType)
      val max = Literal(values.max(ordering), dataType)
      Seq(LessThanOrEqual(column, max), GreaterThanOrEqual(column, min))
    } else Seq.empty

//    columnTransformation match {
//      case l: LengthHashTransformation =>
//        val valuesHashed = values.map(v => (v, l.transform(v.asInstanceOf[UTF8String].toString)))
//
//        // Create ordering for new values
//        val ordering = new Ordering[(Any, Double)] {
//
//          /**
//           * Compare (Any, Double) by using Double comparison
//           */
//          override def compare(x: (Any, Double), y: (Any, Double)): Int =
//            Ordering[Double].compare(x._2, y._2)
//        }
//
//        val min = Literal(valuesHashed.min(ordering)._1, dataType)
//        val max = Literal(valuesHashed.max(ordering)._1, dataType)
//        Seq(LessThanOrEqual(column, max), GreaterThanOrEqual(column, min))
//
//      case _: HashTransformation => // do nothing
//        Seq.empty
//      case _ =>
//        val min = Literal(values.min(ordering), dataType)
//        val max = Literal(values.max(ordering), dataType)
//        Seq(LessThanOrEqual(column, max), GreaterThanOrEqual(column, min))
//    }

  }

  /**
   * Transform IN expression to a Range(>=, <=)
   *
   * Based on Delta DataSkippingReader
   * We match cases in which an IN predicate is called
   * and transform them to a range predicate (>=, <=)
   *
   * @param expression the expression to transform
   * @return the sequence of expressions corresponding to the range
   */

  def transformInExpressions(
      expression: Expression,
      columnTransformer: Transformer): Seq[Expression] = {
    expression match {
      case in @ In(a, values) if in.inSetConvertible =>
        inToRangeExpressions(a, columnTransformer, values.map(_.asInstanceOf[Literal].value))

      // The optimizer automatically converts all but the shortest eligible IN-lists to InSet.
      case InSet(a, values) =>
        inToRangeExpressions(a, columnTransformer, values.toSeq)

      // Treat IN(... subquery ...) as a normal IN-list, since the subquery already ran before now.
      case in: InSubqueryExec =>
        // At this point the subquery has been materialized so it is safe to call get on the Option.
        inToRangeExpressions(in.child, columnTransformer, in.values().get.toSeq)

      // If the Filter involves any other predicates, return without pre-processing
      case other => other :: Nil
    }
  }

}
