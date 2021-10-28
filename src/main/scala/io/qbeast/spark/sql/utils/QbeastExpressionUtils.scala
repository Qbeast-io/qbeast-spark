/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.utils

import io.qbeast.spark.index.Weight
import io.qbeast.spark.model.{Point, QuerySpace, QuerySpaceFromTo, Revision}
import io.qbeast.spark.sql.rules.QbeastMurmur3Hash
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

  private def isQbeastWeightExpression(expression: Expression): Boolean = {
    expression match {
      case BinaryComparison(_: QbeastMurmur3Hash, _) => true
      case _ => false
    }
  }

  private def isQbeastExpression(
      expression: Expression,
      indexedColumns: Seq[String],
      spark: SparkSession): Boolean =
    isQbeastWeightExpression(expression) || hasQbeastColumnReference(
      expression,
      indexedColumns,
      spark)

  /**
   * Analyzes the data filters from the query
   * @param dataFilters filters passed to the relation
   * @param revision the revision of the index
   * @param spark the spark session
   * @return sequence of filters involving qbeast format
   */
  def extractDataFilters(
      dataFilters: Seq[Expression],
      revision: Revision,
      spark: SparkSession): Seq[Expression] = {
    dataFilters.filter(expression =>
      isQbeastExpression(expression, revision.indexedColumns, spark) && !SubqueryExpression
        .hasSubquery(expression))
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
   * Extracts the space of the query
   * @param dataFilters the filters passed by the spark engine
   * @param revision the characteristics of the index
   * @return
   */

  def extractQuerySpace(
      dataFilters: Seq[Expression],
      revision: Revision,
      spark: SparkSession): QuerySpace = {

    // Split conjunctive predicates
    val filters = dataFilters.flatMap(filter => splitConjunctivePredicates(filter))
    val indexedColumns = revision.indexedColumns

    val fromTo = indexedColumns.map(columnName => {

      val columnFilters = filters.filter(hasColumnReference(_, columnName, spark))
      val indexedColumnIndex = revision.indexedColumns.indexOf(columnName)
      val indexedColumnTransformations = revision.transformations(indexedColumnIndex)

      val from = columnFilters
        .collectFirst {
          case GreaterThanOrEqual(_, Literal(value, dataType)) =>
            extractIntValue(value, dataType).doubleValue()
          case EqualTo(_, Literal(value, dataType)) =>
            extractIntValue(value, dataType).doubleValue()
        }
        .getOrElse(indexedColumnTransformations.min)

      val to = columnFilters
        .collectFirst { case LessThan(_, Literal(value, dataType)) =>
          extractIntValue(value, dataType).doubleValue()
        }
        .getOrElse(indexedColumnTransformations.max)

      (from, to)

    })

    val from = Point(fromTo.map(_._1).toVector)
    val to = Point(fromTo.map(_._2).toVector)

    QuerySpaceFromTo(from, to, revision)
  }

  /**
   * Extracts the sampling weight range of the query
   * @param dataFilters the filters passed by the spark engine
   * @return the upper and lower weight bounds (default: Weight.MinValue, Weight.MaxValue)
   */
  def extractWeightRange(dataFilters: Seq[Expression]): (Weight, Weight) = {

    val weightFilters = dataFilters
      .flatMap(filter => splitConjunctivePredicates(filter))
      .filter(isQbeastWeightExpression)

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
