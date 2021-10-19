/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.utils

import io.qbeast.spark.index.Weight
import io.qbeast.spark.model.Point
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object QbeastExpressionUtils {

  private def hasQbeastColumnReference(
      expr: Expression,
      dimensionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    expr.references.forall { r =>
      dimensionColumns.exists(nameEquality(r.name, _))
    }
  }

  private def isQbeastExpression(
      expression: Expression,
      dimensionColumns: Seq[String],
      spark: SparkSession): Boolean =
    expression.children.head.prettyName
      .equals("qbeast_hash") || hasQbeastColumnReference(expression, dimensionColumns, spark)

  /**
   * Analyzes the data filters from the query
   * @param dataFilters filters passed to the relation
   * @return min max weight (default Integer.MINVALUE, Integer.MAXVALUE)
   */
  def extractDataFilters(
      dataFilters: Seq[Expression],
      dimensionColumns: Seq[String],
      spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    dataFilters.partition(expression =>
      isQbeastExpression(expression, dimensionColumns, spark) && !SubqueryExpression.hasSubquery(
        expression))
  }

  /**
   * Split conjuntive predicates from an Expression into different values for the sequence
   * @param condition the Expression to analyze
   * @return the sequence of all predicates
   */

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  private def extractIntValue(v: Any, dataType: DataType): Int = {
    dataType match {
      case ShortType => v.asInstanceOf[Short]
      case IntegerType | DateType => v.asInstanceOf[Int]
      case LongType | TimestampType => v.asInstanceOf[Long].toInt
      case FloatType => v.asInstanceOf[Float].toInt
      case DoubleType => v.asInstanceOf[Double].toInt
      case _: DecimalType => v.asInstanceOf[Decimal].toInt
    }
  }

  private def hasColumnReference(
      expr: Expression,
      columnName: String,
      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    expr.references.forall(r => nameEquality(r.name, columnName))
  }

  /**
   * Extracts the space range of the query
   * @param dataFilters the filters passed by the user
   * @param dimensionColumns the columns indexed with Qbeast
   * @return
   */

  def extractQueryRange(
      dataFilters: Seq[Expression],
      dimensionColumns: Seq[String],
      spark: SparkSession): (Point, Point) = {

    // Split conjunctive predicates
    val filters = dataFilters.flatMap(filter => splitConjunctivePredicates(filter))

    val fromTo = dimensionColumns.map(columnName => {

      val columnFilters = filters.filter(hasColumnReference(_, columnName, spark))

      val from = columnFilters
        .collectFirst {
          case GreaterThanOrEqual(_, Literal(value, dataType)) =>
            extractIntValue(value, dataType)
          case EqualTo(_, Literal(value, dataType)) =>
            extractIntValue(value, dataType)
        }
        .getOrElse(Int.MinValue)
        .doubleValue()

      val to = columnFilters
        .collectFirst { case LessThan(_, Literal(value, dataType)) =>
          extractIntValue(value, dataType)
        }
        .getOrElse(Int.MaxValue)
        .doubleValue()

      (from, to)

    })

    val from = fromTo.map(_._1)
    val to = fromTo.map(_._2)

    (Point(from.toVector), Point(to.toVector))
  }

  def extractWeightRange(filters: Seq[Expression]): (Weight, Weight) = {
    val weightFilters = filters.filter(expression =>
      expression.children.head.prettyName
        .equals("qbeast_hash"))

    val min = weightFilters
      .collectFirst { case GreaterThanOrEqual(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .getOrElse(Int.MinValue)

    val max = weightFilters
      .collectFirst { case LessThan(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .getOrElse(Int.MaxValue)

    (Weight(min), Weight(max))
  }

}
