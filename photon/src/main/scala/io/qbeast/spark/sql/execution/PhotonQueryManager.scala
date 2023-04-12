package io.qbeast.spark.sql.execution

import io.qbeast.core.model._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{
  And,
  EqualTo,
  Expression,
  GreaterThan,
  GreaterThanOrEqual,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Literal,
  SubqueryExpression
}
import org.apache.spark.unsafe.types.UTF8String

object PhotonQueryManager extends QueryManager[QueryOperators] with StagingUtils {

  lazy val spark: SparkSession = SparkSession.active
  lazy val nameEquality: Resolver = spark.sessionState.analyzer.resolver

  private def hasQbeastColumnReference(expr: Expression, indexedColumns: Seq[String]): Boolean = {
    expr.references.forall { r =>
      indexedColumns.exists(nameEquality(r.name, _))
    }
  }

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
   *
   * @param dataFilters filters passed to the relation
   * @param revision    the revision of the index
   * @return sequence of filters involving qbeast format
   */
  private def extractDataFilters(
      dataFilters: Seq[Expression],
      revision: Revision): Seq[Expression] = {
    dataFilters.filter(expression =>
      hasQbeastColumnReference(
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
   *
   * @param dataFilters the filters passed by the spark engine
   * @param revision    the characteristics of the index
   * @return
   */

  private def extractQuerySpace(dataFilters: Seq[Expression], revision: Revision): QuerySpace = {
    // Split conjunctive predicates
    val filters = dataFilters.flatMap(filter => splitConjunctivePredicates(filter))

    // Include all revision space when no filter is applied on its indexing columns
    if (filters.isEmpty) AllSpace()
    else {
      val indexedColumns = revision.columnTransformers.map(_.columnName)

      val (from, to) =
        indexedColumns.map { columnName =>
          // Get the filters related to the column
          val columnFilters = filters.filter(hasColumnReference(_, columnName))

          // Get the coordinates of the column in the filters,
          // if not found, use the overall coordinates
          val columnFrom = columnFilters
            .collectFirst {
              case GreaterThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
              case GreaterThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
              case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
              case IsNull(_) => null
            }

          val columnTo = columnFilters
            .collectFirst {
              case LessThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
              case LessThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
              case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
              case IsNull(_) => null
            }

          (columnFrom, columnTo)
        }.unzip

      QuerySpace(from, to, revision.transformations)
    }
  }

  /**
   * Builds a QuerySpec for a specific revision
   *
   * @param query the QueryOperators involved
   * @param revision the specific Revision
   * @return
   */
  override def buildSpec(query: QueryOperators, revision: Revision): QuerySpec = {

    val querySpace =
      if (isStaging(revision)) EmptySpace()
      else {
        val qbeastFilters = extractDataFilters(query.filters, revision)
        extractQuerySpace(qbeastFilters, revision)
      }
    val weightRange = query.sample match {
      case Some(sample) =>
        val lowerBoundWeight = Weight(sample.lowerBound)
        val upperBoundWeight = Weight(sample.upperBound)
        WeightRange(lowerBoundWeight, upperBoundWeight)
      case None => WeightRange(Weight.MinValue, Weight.MaxValue)
    }

    QuerySpec(weightRange, querySpace)
  }

}
