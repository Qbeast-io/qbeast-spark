package io.qbeast.spark.utils

import io.qbeast.core.model.{
  IntegerDataType,
  Point,
  QTableID,
  QuerySpaceFromTo,
  Revision,
  Weight,
  WeightRange
}
import io.qbeast.core.transform.{LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.expr
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastExpressionUtilsTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

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
    "extract all weight range when expressions is empty" in {

      QbeastExpressionUtils
        .extractWeightRange(Seq.empty) shouldBe WeightRange(Weight.MinValue, Weight.MaxValue)
    }

  it should "filter correctly the expressions for weight" in {

    val weightRange = WeightRange(Weight(3), Weight(8))
    val expression = weightFilters(weightRange)
    QbeastExpressionUtils.extractWeightRange(Seq(expression)) shouldBe weightRange

  }

  "extractQueryRange" should "extract query range" in withSpark(spark => {

    val (from, to) = (3, 8)
    val expression = expr(s"id >= $from and id < $to").expr
    val revision = createRevision()
    QbeastExpressionUtils
      .extractQuerySpace(Seq(expression), revision) shouldBe
      QuerySpaceFromTo(Point(from.toDouble), Point(to.toDouble), revision)

  })

  it should "extract query range when equal to" in withSpark(spark => {

    val revision = createRevision()
    val expression = expr("id == 3").expr

    QbeastExpressionUtils
      .extractQuerySpace(Seq(expression), revision) shouldBe
      QuerySpaceFromTo(Point(3.0), Point(Int.MaxValue), revision)

  })

  it should "extract all query range when expressions is empty" in withSpark(spark => {

    val revision = createRevision()
    QbeastExpressionUtils
      .extractQuerySpace(Seq.empty, revision) shouldBe
      QuerySpaceFromTo(Point(Int.MinValue), Point(Int.MaxValue), revision)
  })

  it should "not process disjunctive predicates" in withSpark(spark => {

    val revision = createRevision()
    val expression = expr(s"id >= 3 OR id < 8").expr

    QbeastExpressionUtils
      .extractQuerySpace(Seq(expression), revision) shouldBe
      QuerySpaceFromTo(Point(Int.MinValue), Point(Int.MaxValue), revision)

  })

  "extractDataFilters" should "extract qbeast filters correctly" in withSpark(spark => {
    val revision = createRevision()
    val rangeExpression = expr(s"id >= 3 OR id < 8").expr
    val weightExpression = weightFilters(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)))
    val expressions = Seq(rangeExpression, weightExpression)

    QbeastExpressionUtils
      .extractDataFilters(expressions, revision) shouldBe ((expressions, Seq.empty))

  })
}
