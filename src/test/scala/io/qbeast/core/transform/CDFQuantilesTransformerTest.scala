package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.QDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFQuantilesTransformerTest extends AnyFlatSpec with Matchers {

  private val orderedDataType = IntegerDataType

  private val invalidDataType = new QDataType {
    override def name: String = "non-int"
  }

  "CDFQuantilesTransformer" should "return correct stats for given column name and data type" in {
    val transformer = CDFQuantilesTransformer("testColumn", IntegerDataType)
    val defaultQuantiles = IndexedSeq(-0.1, -0.2, -0.3, -0.4, -0.5, -0.6, -0.7, -0.8, -0.9, -1.0)
    val stats = transformer.stats
    stats.statsNames should contain("testColumn_quantiles")
    stats.statsSqlPredicates should contain(
      s"${defaultQuantiles.mkString("Array(", ", ", ")")} AS testColumn_quantiles")
  }

  it should "throw IllegalArgumentException for invalid data type" in {
    assertThrows[IllegalArgumentException] {
      CDFQuantilesTransformer("testColumn", invalidDataType)
    }
  }

  it should "return correct transformation for valid data type" in {
    val transformer = CDFQuantilesTransformer("testColumn", orderedDataType)
    val transformation = transformer.makeTransformation(_ => Seq.empty)
    transformation shouldBe a[CDFQuantilesTransformation]
  }

}
