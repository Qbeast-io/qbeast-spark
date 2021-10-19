package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.Weight
import io.qbeast.spark.model.Point
import io.qbeast.spark.sql.rules.QbeastMurmur3Hash
import io.qbeast.spark.sql.utils.QbeastExpressionUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastExpressionUtilsTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  def weightFilters(from: Weight, to: Weight): Seq[Expression] = {
    val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("id").expr))
    val lessThan = LessThan(qbeast_hash, Literal(to.value))
    val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(from.value))
    Seq(lessThan, greaterThanOrEqual)
  }

  def rangeFilters(from: Any, to: Any, columnName: String): Seq[Expression] = {

    val column = new Column(columnName).expr
    val lessThan = LessThan(column, Literal(to))
    val greaterThanOrEqual = GreaterThanOrEqual(column, Literal(from))
    Seq(lessThan, greaterThanOrEqual)
  }

  "QbeastExpressionUtils" should
    "extract all weight range when expressions is empty" in withSpark(spark => {

      QbeastExpressionUtils
        .extractWeightRange(Seq.empty) shouldBe ((Weight.MinValue, Weight.MaxValue))
    })

  it should "filter correctly the expressions for weight" in withSpark(_ => {

    val (from, to) = (Weight(3), Weight(8))
    val expression = weightFilters(from, to)
    QbeastExpressionUtils.extractWeightRange(expression) shouldBe ((from, to))

  })

  it should "extract query range" in withSpark(spark => {

    val (from, to) = (3, 8)
    val columnName = "id"
    val expression = rangeFilters(from, to, columnName)
    QbeastExpressionUtils
      .extractQueryRange(expression, Seq(columnName), spark) shouldBe ((Point(from), Point(to)))

  })

  it should "extract qbeast filters correctly" in withSpark(spark => {

    val (from, to) = (3, 8)
    val columnName = "id"
    val rangeExpression = rangeFilters(from, to, columnName)
    val weightExpression = weightFilters(Weight(from), Weight(to))
    val expression = rangeExpression ++ weightExpression

    QbeastExpressionUtils
      .extractDataFilters(expression, Seq(columnName), spark)
      ._1 shouldBe expression

  })
}
