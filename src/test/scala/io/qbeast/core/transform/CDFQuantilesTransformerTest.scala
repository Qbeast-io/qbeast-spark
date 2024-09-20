package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.apache.spark.sql.AnalysisException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFQuantilesTransformerTest extends AnyFlatSpec with Matchers {

  "CDFQuantilesTransformer" should "return correct stats for given column name and data type" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    val stats = transformer.stats
    stats.statsNames should contain("testColumn_quantiles")
    stats.statsSqlPredicates shouldBe empty
  }

  it should "return empty transformation for empty stats" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    val transformation = transformer.makeTransformation(_ => null)
    transformation shouldBe a[EmptyTransformation]
  }

  it should "return correct transformation for non-empty stats" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    val transformation = transformer.makeTransformation(_ => Seq(0.0, 0.25, 0.5, 0.75, 1.0))
    transformation shouldBe a[CDFNumericQuantilesTransformation]
    transformation.asInstanceOf[CDFNumericQuantilesTransformation].quantiles shouldBe Seq(0.0,
      0.25, 0.5, 0.75, 1.0)
  }

  it should "fail to create transformation for non-empty stats with wrong type" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    an[AnalysisException] should be thrownBy transformer.makeTransformation(_ =>
      Seq("0.0", "0.25", "0.5", "0.75", "1.0"))

  }

  it should "throw an exception for empty sequence of bins" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    an[AnalysisException] should be thrownBy transformer.makeTransformation(_ => Seq.empty)
  }

}
