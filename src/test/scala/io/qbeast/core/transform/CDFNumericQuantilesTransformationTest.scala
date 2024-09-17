package io.qbeast.core.transform

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFNumericQuantilesTransformationTest extends AnyFlatSpec with Matchers {

  val orderedDataTypeTest = IntegerDataType

  "CDFNumericQuantilesTransformer" should "return correct transformation for found index" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(2) should be(0.5)
  }

  it should "return correct transformation for insertion point at start" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(0) should be(0.0)
  }

  it should "return correct transformation for insertion point at end" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(4) should be(1.0)
  }

  it should "return correct transformation for insertion point in middle" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1.0, 2.0, 3.0), DoubleDataType)
    qt.transform(1.5) should be(0.0)
  }

  it should "return true when quantiles are different and neither is default" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = CDFQuantilesTransformation(IndexedSeq(4, 5, 6), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(true)
  }

  it should "return false when quantiles are the same and neither is default" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(false)
  }

  "merge" should "return other transformation when it is a QuantileTransformation" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val otherQt = CDFQuantilesTransformation(IndexedSeq(4, 5, 6), orderedDataTypeTest)
    qt.merge(otherQt) should be(otherQt)
  }

  it should "return current transformation when other is not a QuantileTransformation" in {
    val qt = CDFQuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val other = new LinearTransformation(1, 10, 7, orderedDataTypeTest)
    qt.merge(other) should be(qt)
  }

}
