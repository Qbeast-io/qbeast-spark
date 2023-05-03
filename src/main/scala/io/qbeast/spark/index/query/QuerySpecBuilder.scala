/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import org.apache.spark.sql.catalyst.expressions.{
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
import org.apache.spark.sql.types.IntegerType

/**
 * Builds a query specification from a set of filters
 * @param sparkFilters the filters
 */
private[spark] class QuerySpecBuilder(sparkFilters: Seq[Expression])
    extends Serializable
    with StagingUtils
    with QueryFiltersUtils {

  /**
   * Extracts the data filters from the query that can be used by qbeast
   *
   * @param dataFilters filters passed to the relation
   * @param revision    the revision of the index
   * @return sequence of filters involving qbeast format
   */
  def extractQbeastFilters(dataFilters: Seq[Expression], revision: Revision): QbeastFilters = {
    // Get the indexed column names
    val indexedColumns = revision.columnTransformers.map(_.columnName)

    // Split the conjunctive (AND) predicates, if any
    // We could Recieve data filters like
    // qbeast_hash(col1) >= 183483983773 AND col1 > 10 AND col1 < 40
    // This should be split in Seq((qbeast_hash), (col1 > 10), (col < 40))

    val conjunctiveSplit =
      dataFilters
        .filter(!SubqueryExpression.hasSubquery(_))
        .flatMap(splitConjunctiveExpressions)

    // Extract the weight filter from conjunctiveSplit
    val weightFilters = conjunctiveSplit.filter(isQbeastWeightExpression)

    // Transform the remaining filters into Range Predicates (>=, <=)
    val transformedFilters = conjunctiveSplit.flatMap(transformInExpressions)

    // And filter those that involve any Qbeast Indexed Column
    val queryFilters = transformedFilters.filter(hasQbeastColumnReference(_, indexedColumns))

    QbeastFilters(weightFilters, queryFilters)
  }

  /**
   * Extracts the sequence of query spaces
   * That should be unioned after
   *
   * This RangeExpressions evaluate from >, >= to <, <= any indexed column
   *
   * @param rangeExpressions the expressions passed by the spark engine
   * @param revision the revision of the index
   * @return
   */

  def extractQuerySpace(rangeExpressions: Seq[Expression], revision: Revision): QuerySpace = {

    // The predicates should not be empty
    assert(rangeExpressions.nonEmpty)

    val indexedColumns = revision.columnTransformers.map(_.columnName)

    val (from, to) =
      indexedColumns.map { columnName =>
        // Get the filters related to the column
        val columnFilters = rangeExpressions.filter(hasColumnReference(_, columnName))

        // Get the coordinates of the column in the filters,
        // if not found, use the overall coordinates
        val columnFrom = columnFilters
          .collectFirst {
            case GreaterThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case GreaterThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case LessThan(Literal(value, _), _) => sparkTypeToCoreType(value)
            case LessThanOrEqual(Literal(value, _), _) => sparkTypeToCoreType(value)
            case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case EqualTo(Literal(value, _), _) => sparkTypeToCoreType(value)
            case IsNull(_) => null
          }

        val columnTo = columnFilters
          .collectFirst {
            case LessThan(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case LessThanOrEqual(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case GreaterThan(Literal(value, _), _) => sparkTypeToCoreType(value)
            case GreaterThanOrEqual(Literal(value, _), _) => sparkTypeToCoreType(value)
            case EqualTo(_, Literal(value, _)) => sparkTypeToCoreType(value)
            case EqualTo(Literal(value, _), _) => sparkTypeToCoreType(value)
            case IsNull(_) => null
          }

        (columnFrom, columnTo)
      }.unzip

    QuerySpace(from, to, revision.transformations)

  }

  /**
   * Extracts the sampling weight range of the query
   * @param dataFilters the filters passed by the spark engine
   * @return the upper and lower weight bounds (default: Weight.MinValue, Weight.MaxValue)
   */
  def extractWeightRange(dataFilters: Seq[Expression]): WeightRange = {

    val weightFilters = dataFilters.filter(isQbeastWeightExpression)

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
   * @return the non overlapping sequence of QuerySpecs
   */

  def build(revision: Revision): Seq[QuerySpec] = {

    // Extract a QbeastFilters object from the sparkFilters and the particular revision
    // QbeastFilters contains the queryFilters and the weightFilters
    val qbeastFilters = extractQbeastFilters(sparkFilters, revision)
    // Extract the weight range associated to the Sample
    // If no sample if present, weightRange would be (Int.MinValue, Int.MaxValue)
    val weightRange = extractWeightRange(qbeastFilters.weightFilters)

    if (isStaging(revision)) {
      Seq(QuerySpec(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)), EmptySpace()))
    } else if (qbeastFilters.queryFilters.isEmpty) {
      Seq(QuerySpec(weightRange, AllSpace()))
    } else {

      // First split disjunctive predicates
      // To generate a QuerySpec for each space
      val (disjunctivePredicates, conjunctivePredicates) =
        qbeastFilters.queryFilters.partition(isDisjunctiveExpression)

      // Process the conjunctive predicates, if any
      val processedPredicates =
        if (conjunctivePredicates.isEmpty) Seq.empty
        else Seq(QuerySpec(weightRange, extractQuerySpace(conjunctivePredicates, revision)))

      // Process each disjunctive predicate as a different QuerySpec
      val processedDisjunctivePredicates = disjunctivePredicates
        .flatMap(splitDisjunctiveExpressions)
        .map(f => {
          QuerySpec(weightRange, extractQuerySpace(splitConjunctiveExpressions(f), revision))
        })

      // Add both sets of predicates to the final query
      val querySpecs = processedDisjunctivePredicates ++ processedPredicates

      // Discard overlapping query specs to avoid retrieving the same set of results
      // TODO this might be done while processing the predicates
      var nonOverlappingQuerySpecs = Seq.empty[QuerySpec]
      querySpecs.foreach(querySpec => {
        val space = querySpec.querySpace
        val existsOverlapping = nonOverlappingQuerySpecs.exists(_.querySpace.contains(space))
        if (!existsOverlapping) {
          nonOverlappingQuerySpecs = nonOverlappingQuerySpecs ++ Seq(querySpec)
        }
      })

      nonOverlappingQuerySpecs
+
    }
  }

}
