package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFNumericQuantilesTransformationTest extends AnyFlatSpec with Matchers {

  val orderedDataTypeTest = IntegerDataType

  "CDFNumericQuantilesTransformer" should "return correct transformation for found index" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(2) should be(0.5)
  }

  it should "return correct transformation for insertion point at start" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(0) should be(0.0)
  }

  it should "return correct transformation for insertion point at end" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(4) should be(1.0)
  }

  it should "return correct transformation for insertion point in the bin" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 3, 5), orderedDataTypeTest)
    // 2 is between 1 and 3, so it should be 0.25 (fraction = 2-1 / 3-1 = 0.5. -> 0 + fraction(0.5) / 2 = 0.25)
    qt.transform(2) should be(0.25)
  }

  it should "return correct transformation for insertion point in the bin with repeated values" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 1, 3, 5), orderedDataTypeTest)
    // 2 is between 1 and 3 so it should be 0.5
    qt.transform(2) should be(0.5)
  }

  it should "return correct transformation point for all values inside the bin" in {
    val quantiles = IndexedSeq(1, 1, 1, 1, 1, 1, 100, 100, 100).map(_.toDouble)
    val qt =
      CDFNumericQuantilesTransformation(quantiles, orderedDataTypeTest)
    val valuesToTest = 2.to(99)
    val maxIndexQuantiles = quantiles.size - 1
    val results = valuesToTest.map { value =>
      val transformation = qt.transform(value)
      transformation shouldBe >=(5 / maxIndexQuantiles.toDouble)
      transformation shouldBe <=(6 / maxIndexQuantiles.toDouble)
      transformation
    }
    results.sorted should be(results)
  }

  it should "return true when quantiles are different and neither is default" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = CDFNumericQuantilesTransformation(IndexedSeq(4, 5, 6), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(true)
  }

  it should "return false when quantiles are the same and neither is default" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(false)
  }

  "merge" should "return other transformation when it is a QuantileTransformation" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val otherQt = CDFNumericQuantilesTransformation(IndexedSeq(4, 5, 6), orderedDataTypeTest)
    qt.merge(otherQt) should be(otherQt)
  }

  it should "return current transformation when other is not a QuantileTransformation" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val other = new LinearTransformation(1, 10, 7, orderedDataTypeTest)
    qt.merge(other) should be(qt)
  }

}
