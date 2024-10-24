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
package io.qbeast.spark.index.query

import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.daysToMicros
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.TimeUnit

trait QueryFiltersUtils {

  lazy val spark: SparkSession = SparkSession.active
  lazy val nameEquality: Resolver = spark.sessionState.analyzer.resolver

  /**
   * Checks if an expression references to a certain column
   * @param expr
   *   the Expression
   * @param columnName
   *   the name of the column
   * @return
   */
  def hasColumnReference(expr: Expression, columnName: String): Boolean = {
    expr.references.forall(r => nameEquality(r.name, columnName))
  }

  /**
   * Checks if an expression references to any of the Qbeast Indexed Columns
   * @param expr
   *   the Expression
   * @param indexedColumns
   *   the current indexed columns
   * @return
   */
  def hasQbeastColumnReference(expr: Expression, indexedColumns: Seq[String]): Boolean = {
    expr.references.forall { r =>
      indexedColumns.exists(nameEquality(r.name, _))
    }
  }

  /**
   * Checks if an Expression is a Qbeast Weight filter
   * @param expression
   *   the Expression
   * @return
   */
  def isQbeastWeightExpression(expression: Expression): Boolean = {
    expression match {
      case BinaryComparison(_: QbeastMurmur3Hash, _) => true
      case _ => false
    }
  }

  /**
   * Returns whether given expressions contain a Qbeast weight expression, i.e. sampling clause.
   * Subqueries are not supported.
   *
   * @param expressions
   *   the expressions to test
   * @return
   *   the expression has Qbeast weight expression
   */
  def haveQbeastWeightExpression(expressions: Seq[Expression]): Boolean =
    expressions
      .filterNot(SubqueryExpression.hasSubquery)
      .flatMap(splitConjunctiveExpressions)
      .exists(isQbeastWeightExpression)

  /**
   * Checks if an Expression is Disjunctive (OR)
   * @param condition
   *   the expression
   * @return
   */
  def isDisjunctiveExpression(condition: Expression): Boolean = {
    condition.isInstanceOf[Or]
  }

  /**
   * Recursively split Disjunctive operators (AND) in an expression
   *
   * @param condition
   *   the expression to evaluate
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
   * Recursively split Conjunctive operators (AND) in an expression
   *
   * @param condition
   *   the expression to evaluate
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
   * Convert a Literal value from Spark to a Qbeast/Scala core type
   * @param l
   *   the Literal to convert
   * @return
   */

  def sparkTypeToCoreType(l: Literal): Any = {

    (l.value, l.dataType) match {
      case (int: Integer, _: DateType) =>
        // convert DateType to Milliseconds
        lazy val zoneId = getZoneId(SQLConf.get.sessionLocalTimeZone)
        val dateInMicros = daysToMicros(int, zoneId)
        val dateInMillis = TimeUnit.MICROSECONDS.toMillis(dateInMicros)
        dateInMillis
      case (long: Long, _: TimestampType) =>
        // convert Timestamp from Microseconds to Milliseconds
        TimeUnit.MICROSECONDS.toMillis(long)
      case (s: UTF8String, _) => s.toString
      case _ => l.value
    }
  }

  /**
   * Transform an expression Based on Delta DataSkippingReader Since we already know this filter
   * are eligible for skipping we directly output the RangePredicate
   *
   * @param column
   * @param values
   * @return
   */
  def inToRangeExpressions(column: Expression, values: Seq[Any]): Seq[Expression] = {

    val dataType = column.dataType
    lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)
    val min = Literal(values.min(ordering), dataType)
    val max = Literal(values.max(ordering), dataType)
    Seq(LessThanOrEqual(column, max), GreaterThanOrEqual(column, min))

  }

  /**
   * Transform IN expression to a Range(>=, <=)
   *
   * Based on Delta DataSkippingReader We match cases in which an IN predicate is called and
   * transform them to a range predicate (>=, <=)
   *
   * @param expression
   *   the expression to transform
   * @return
   *   the sequence of expressions corresponding to the range
   */

  def transformInExpressions(expression: Expression): Seq[Expression] = {
    expression match {
      case in @ In(a, values) if in.inSetConvertible =>
        inToRangeExpressions(a, values.map(_.asInstanceOf[Literal].value))

      // The optimizer automatically converts all but the shortest eligible IN-lists to InSet.
      case InSet(a, values) =>
        inToRangeExpressions(a, values.toSeq)

      // Treat IN(... subquery ...) as a normal IN-list, since the subquery already ran before now.
      case in: InSubqueryExec =>
        // At this point the subquery has been materialized so it is safe to call get on the Option.
        inToRangeExpressions(in.child, in.values().get.toSeq)

      // If the Filter involves any other predicates, return without pre-processing
      case other => other :: Nil
    }
  }

}
