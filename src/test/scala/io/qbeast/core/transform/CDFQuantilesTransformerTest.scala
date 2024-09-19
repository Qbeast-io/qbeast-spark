package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFQuantilesTransformerTest extends AnyFlatSpec with Matchers {

  private val orderedDataType = IntegerDataType

  "CDFQuantilesTransformer" should "return correct stats for given column name and data type" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    val stats = transformer.stats
    stats.statsNames should contain("testColumn_quantiles")
    stats.statsSqlPredicates should contain(s"array() AS testColumn_quantiles")
  }

  it should "return correct transformation for valid data type" in {
    val transformer = CDFQuantilesTransformer("testColumn", orderedDataType)
    val transformation = transformer.makeTransformation(_ => Seq.empty)
    transformation shouldBe a[CDFQuantilesTransformation]
  }

}
