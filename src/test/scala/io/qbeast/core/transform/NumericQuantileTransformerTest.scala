package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.QDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NumericQuantileTransformerTest extends AnyFlatSpec with Matchers {

  private val orderedDataType = new OrderedDataType {
    override val ordering: Numeric[Any] = IntegerDataType.ordering

    override val defaultQuantiles: IndexedSeq[Any] = Seq(1, 2, 3).toIndexedSeq

    override def name: String = "int"
  }

  private val nonOrderedDataType = new QDataType {
    override def name: String = "non-int"
  }

  "NumericQuantilesTransformer" should "return correct stats for given column name and data type" in {
    val transformer = NumericQuantileTransformer("testColumn", orderedDataType)
    val stats = transformer.stats
    stats.statsNames should contain("testColumn_percentiles")
    stats.statsSqlPredicates should contain(
      "approx_percentile(testColumn) AS testColumn_percentiles")
  }

  it should "throw IllegalArgumentException for invalid data type" in {
    val transformer = NumericQuantileTransformer("testColumn", nonOrderedDataType)
    assertThrows[IllegalArgumentException] {
      transformer.makeTransformation(_ => new AnyRef)
    }
  }

  it should "return correct transformation for valid data type" in {
    val transformer = NumericQuantileTransformer("testColumn", orderedDataType)
    val transformation = transformer.makeTransformation(_ => Seq.empty)
    transformation shouldBe a[NumericQuantileTransformation]
  }

  it should "return default quantiles for empty sequence" in {
    val transformer = NumericQuantileTransformer("testColumn", orderedDataType)
    println(orderedDataType.toString)
    val transformation =
      transformer.makeTransformation(_ => Seq.empty).asInstanceOf[NumericQuantileTransformation]

    println(transformation.toString)
    transformation.approxQuantiles should be(orderedDataType.defaultQuantiles)
  }

}
