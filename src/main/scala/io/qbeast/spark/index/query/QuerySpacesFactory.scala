/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model.{AllSpace, QuerySpace, Revision}
import io.qbeast.spark.internal.expressions.QbeastSample
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{
  Or,
  Expression,
  And,
  SubqueryExpression,
  LessThan,
  AttributeReference,
  Literal,
  LessThanOrEqual,
  GreaterThanOrEqual,
  GreaterThan,
  EqualTo,
  IsNull,
  In,
  InSet
}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.types.{NumericType, StringType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

/**
 * Factory fro creating query spaces for different index revisions.
 *
 * @param filters the query filters
 */
private[query] class QuerySpacesFactory(spark: SparkSession, filters: Seq[Expression]) {
  private lazy val cnf: Seq[Seq[Seq[Expression]]] = normalizeFilters(filters)
  private lazy val resolver: Resolver = spark.sessionState.analyzer.resolver

  def newQuerySpaces(revision: Revision): Seq[Seq[QuerySpace]] = {
    cnf.map(newUnion(revision))
  }

  private def normalizeFilters(filters: Seq[Expression]): Seq[Seq[Seq[Expression]]] = {
    val builder = Seq.newBuilder[Seq[Seq[Expression]]]
    val (orFilters, otherFilters) =
      filters.filterNot(SubqueryExpression.hasSubquery).partition(_.isInstanceOf[Or])
    for (filter <- orFilters) {
      builder += normalizeDisjunction(Seq(filter))
    }
    builder += Seq(normalizeConjunction(otherFilters))
    builder.result()
  }

  private def normalizeDisjunction(filters: Seq[Expression]): Seq[Seq[Expression]] = {
    filters.flatMap(decomposeOr).map(Seq(_)).map(normalizeConjunction)
  }

  private def decomposeOr(filter: Expression): Seq[Expression] = filter match {
    case Or(left, right) => decomposeOr(left) ++ decomposeOr(right)
    case _ => Seq(filter)
  }

  private def normalizeConjunction(filters: Seq[Expression]): Seq[Expression] = {
    filters
      .flatMap(decomposeAnd)
      .flatMap(transformIn)
      .filter(supports)
      .filterNot(_.isInstanceOf[QbeastSample])
  }

  private def decomposeAnd(filter: Expression): Seq[Expression] = filter match {
    case And(left, right) => decomposeAnd(left) ++ decomposeAnd(right)
    case _ => Seq(filter)
  }

  private def supports(filter: Expression): Boolean = filter match {
    // Comparison expression is only supported if one part is column reference
    // and the other part is a numeric literal
    case LessThan(_: AttributeReference, Literal(_, _: NumericType)) => true
    case LessThan(Literal(_, _: NumericType), _: AttributeReference) => true
    case GreaterThan(_: AttributeReference, Literal(_, _: NumericType)) => true
    case GreaterThan(Literal(_, _: NumericType), _: AttributeReference) => true
    case LessThanOrEqual(_: AttributeReference, Literal(_, _: NumericType)) => true
    case LessThanOrEqual(Literal(_, _: NumericType), _: AttributeReference) => true
    case GreaterThanOrEqual(_: AttributeReference, Literal(_, _: NumericType)) => true
    case GreaterThanOrEqual(Literal(_, _: NumericType), _: AttributeReference) => true
    // Equality expression is only supported if one part is a column reference
    // and the other part is string or numeric literal
    case EqualTo(_: AttributeReference, Literal(_, _: NumericType)) => true
    case EqualTo(_: AttributeReference, Literal(_, _: StringType)) => true
    case EqualTo(Literal(_, _: NumericType), _: AttributeReference) => true
    case EqualTo(Literal(_, _: StringType), _: AttributeReference) => true
    // In expression is supported if it uses column referencs of numeric type
    // and has one the following form: non-optimized in with literals, optimized
    // in set, already executed subquery
    case in @ In(r: AttributeReference, _) =>
      r.dataType.isInstanceOf[NumericType] && in.inSetConvertible
    case InSet(r: AttributeReference, _) => r.dataType.isInstanceOf[NumericType]
    case e: InSubqueryExec => e.child.dataType.isInstanceOf[NumericType]
    // Is Null expression is supported for column references
    case IsNull(_: AttributeReference) => true
    case _ => false
  }

  private def transformIn(filter: Expression): Seq[Expression] = {
    val (column, values) = filter match {
      case in @ In(column, literals) if in.inSetConvertible =>
        (column, literals.map(_.asInstanceOf[Literal].value))
      case InSet(column, values) => (column, values.toSeq)
      case in: InSubqueryExec => (in.child, in.values().get.toSeq)
      case _ => return Seq(filter)
    }
    if (values.isEmpty) {
      return Seq(filter)
    }
    val dataType = column.dataType
    lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)
    val min = Literal(values.min(ordering), dataType)
    val max = Literal(values.max(ordering), dataType)
    Seq(GreaterThanOrEqual(column, min), LessThanOrEqual(column, max))
  }

  private def newUnion(revision: Revision)(disjunction: Seq[Seq[Expression]]): Seq[QuerySpace] = {
    disjunction.map(newQuerySpace(revision))
  }

  private def newQuerySpace(revision: Revision)(conjunction: Seq[Expression]): QuerySpace = {
    if (conjunction.isEmpty) {
      return AllSpace()
    }
    val columnNames = revision.columnTransformers.map(_.columnName)
    val (from, to) = columnNames.map(name => getColumnFromTo(name, conjunction)).unzip
    QuerySpace(from, to, revision.transformations)
  }

  private def getColumnFromTo(
      columnName: String,
      conjunction: Seq[Expression]): (Option[Any], Option[Any]) = {
    val columnFilters = conjunction.filter(filter => isColumnAware(filter, columnName))

    val from = columnFilters
      .collectFirst {
        case LessThan(Literal(value, _), _) => value
        case LessThanOrEqual(Literal(value, _), _) => value
        case GreaterThan(_, Literal(value, _)) => value
        case GreaterThanOrEqual(_, Literal(value, _)) => value
        case EqualTo(Literal(value, _), _) => value
        case EqualTo(_, Literal(value, _)) => value
        case _: IsNull => null
      }
      .map(convertToScala)

    val to = columnFilters
      .collectFirst {
        case LessThan(_, Literal(value, _)) => value
        case LessThanOrEqual(_, Literal(value, _)) => value
        case GreaterThan(Literal(value, _), _) => value
        case GreaterThanOrEqual(Literal(value, _), _) => value
        case EqualTo(Literal(value, _), _) => value
        case EqualTo(_, Literal(value, _)) => value
        case _: IsNull => null
      }
      .map(convertToScala)

    (from, to)
  }

  private def isColumnAware(filter: Expression, columnName: String): Boolean =
    filter.references.exists(r => resolver(columnName, r.name))

  private def convertToScala(value: Any): Any = value match {
    case s: UTF8String => s.toString()
    case _ => value
  }

}
