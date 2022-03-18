/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.spark.index.query
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  And,
  BinaryComparison,
  EqualTo,
  Expression,
  GreaterThanOrEqual,
  IsNull,
  LessThan,
  Literal,
  SubqueryExpression
}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Builds a query specification from a set of filters
 * @param sparkFilters the filters
 */
private[spark] class QuerySpecBuilder(sparkFilters: Seq[Expression]) extends Serializable {

  lazy val spark = SparkSession.active
  lazy val nameEquality = spark.sessionState.analyzer.resolver

  private def hasQbeastColumnReference(expr: Expression, indexedColumns: Seq[String]): Boolean = {
    expr.references.forall { r =>
      indexedColumns.exists(nameEquality(r.name, _))
    }
  }

  private def isQbeastWeightExpression(expression: Expression): Boolean = {
    expression match {
      case BinaryComparison(_: QbeastMurmur3Hash, _) => true
      case _ => false
    }
  }

  private def isQbeastExpression(expression: Expression, indexedColumns: Seq[String]): Boolean =
    isQbeastWeightExpression(expression) || hasQbeastColumnReference(expression, indexedColumns)

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  private def hasColumnReference(expr: Expression, columnName: String): Boolean = {
    expr.references.forall(r => nameEquality(r.name, columnName))
  }

  /**
   * Extracts the data filters from the query that can be used by qbeast
   * @param dataFilters filters passed to the relation
   * @param revision the revision of the index
   * @return sequence of filters involving qbeast format
   */
  def extractDataFilters(dataFilters: Seq[Expression], revision: Revision): Seq[Expression] = {
    dataFilters.filter(expression =>
      isQbeastExpression(
        expression,
        revision.columnTransformers.map(_.columnName)) && !SubqueryExpression
        .hasSubquery(expression))
  }

  private def sparkTypeToCoreType(value: Any): Any = {
    value match {
      case s: UTF8String => s.toString
      case _ => value
    }
  }

  /**
   * Extracts the space of the query
   * @param dataFilters the filters passed by the spark engine
   * @param revision the characteristics of the index
   * @return
   */

  def extractQuerySpace(dataFilters: Seq[Expression], revision: Revision): QuerySpace = {

    // Split conjunctive predicates
    val filters = dataFilters.flatMap(filter => splitConjunctivePredicates(filter))

    val indexedColumns = revision.columnTransformers.map(_.columnName)
    val fromTo =
      indexedColumns.map { columnName =>
        // Get the filters related to the column
        val columnFilters = filters.filter(hasColumnReference(_, columnName))

        // Get the coordinates of the column in the filters,
        // if not found, use the overall coordinates
        val from = columnFilters
          .collectFirst {
            case GreaterThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case IsNull(_) => null
          }

        val to = columnFilters
          .collectFirst {
            case LessThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case IsNull(_) => null
          }

        (from, to)
      }

    val from = fromTo.map(_._1)
    val to = fromTo.map(_._2)
    QuerySpaceFromTo(from, to, revision.transformations)
  }

  /**
   * Extracts the sampling weight range of the query
   * @param dataFilters the filters passed by the spark engine
   * @return the upper and lower weight bounds (default: Weight.MinValue, Weight.MaxValue)
   */
  def extractWeightRange(dataFilters: Seq[Expression]): WeightRange = {

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

    WeightRange(Weight(min), Weight(max))
  }

  /**
   * Builds the QuerySpec for the desired Revision
   * @param revision the revision
   * @return the QuerySpec
   */

  def build(revision: Revision): QuerySpec = {
    val qbeastFilters = extractDataFilters(sparkFilters, revision)
    query.QuerySpec(extractWeightRange(qbeastFilters), extractQuerySpace(qbeastFilters, revision))
  }

}
