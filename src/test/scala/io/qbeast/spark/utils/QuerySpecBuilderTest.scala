package io.qbeast.spark.utils

import io.qbeast.core.model.{
  IntegerDataType,
  QTableID,
  QuerySpaceFromTo,
  Revision,
  Weight,
  WeightRange
}
import io.qbeast.core.transform.{LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.{QuerySpecBuilder}
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.expr
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QuerySpecBuilderTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  private def weightFilters(weightRange: WeightRange): Expression = {
    val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("id").expr))
    val lessThan = LessThan(qbeast_hash, Literal(weightRange.to.value))
    val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(weightRange.from.value))
    And(lessThan, greaterThanOrEqual)
  }

  private def createRevision() = {
    val transformations =
      Seq(LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType)).toIndexedSeq
    val columnTransformers = Seq(Transformer("linear", "id", IntegerDataType)).toIndexedSeq

    Revision(
      0,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)
  }

  "extractWeightRange" should
    "extract all weight range when expressions is empty" in withSpark(spark => {
      val revision = createRevision()
      val querySpec = new QuerySpecBuilder(Seq.empty).build(revision)

      querySpec.weightRange shouldBe WeightRange(Weight.MinValue, Weight.MaxValue)
    })

  it should "filter correctly the expressions for weight" in withSpark(spark => {
    val weightRange = WeightRange(Weight(3), Weight(8))
    val expression = weightFilters(weightRange)
    val revision = createRevision()
    val querySpec = new QuerySpecBuilder(Seq(expression)).build(revision)
    querySpec.weightRange shouldBe weightRange

  })

  "extractQueryRange" should "extract query range" in withSpark(spark => {

    val (from, to) = (3, 8)
    val expression = expr(s"id >= $from and id < $to").expr
    val revision = createRevision()
    val querySpec = new QuerySpecBuilder(Seq(expression)).build(revision)
    querySpec.querySpace shouldBe QuerySpaceFromTo(
      Seq(Some(from)),
      Seq(Some(to)),
      revision.transformations)

  })

  it should "extract query range when equal to" in withSpark(spark => {

    val revision = createRevision()
    val expression = expr("id == 3").expr
    val querySpec = new QuerySpecBuilder(Seq(expression)).build(revision)

    querySpec.querySpace shouldBe
      QuerySpaceFromTo(Seq(Some(3.0)), Seq(None), revision.transformations)

  })

  it should "extract all query range when expressions is empty" in withSpark(spark => {

    val revision = createRevision()
    val querySpec = new QuerySpecBuilder(Seq.empty).build(revision)
    querySpec.querySpace shouldBe QuerySpaceFromTo(Seq(None), Seq(None), revision.transformations)
  })

  it should "not process disjunctive predicates" in withSpark(spark => {

    val revision = createRevision()
    val expression = expr(s"id >= 3 OR id < 8").expr
    val querySpec = new QuerySpecBuilder(Seq(expression)).build(revision)
    querySpec.querySpace shouldBe
      QuerySpaceFromTo(Seq(None), Seq(None), revision.transformations)

  })

  "extractDataFilters" should "extract qbeast filters correctly" in withSpark(spark => {
    val revision = createRevision()
    val rangeExpression = expr(s"id >= 3 OR id < 8").expr
    val weightExpression = weightFilters(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)))
    val expressions = Seq(rangeExpression, weightExpression)
    val querySpecBuilder = new QuerySpecBuilder(expressions)

    querySpecBuilder.extractDataFilters(expressions, revision) shouldBe expressions

  })
}
