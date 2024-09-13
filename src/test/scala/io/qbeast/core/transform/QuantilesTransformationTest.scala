package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QuantilesTransformationTest extends AnyFlatSpec with Matchers {

  val orderedDataTypeTest = IntegerDataType

  "transform" should "return correct transformation for found index" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(2) should be(0.5)
  }

  it should "return correct transformation for insertion point at start" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(0) should be(0.0)
  }

  it should "return correct transformation for insertion point at end" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(4) should be(1.0)
  }

  it should "return correct transformation for insertion point in middle" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.transform(1.5) should be(0.5)
  }

  "isSupersededBy" should "return true when new transformation is not default and current is default" in {
    val qt = QuantilesTransformation(orderedDataTypeTest.defaultQuantiles, orderedDataTypeTest)
    val newQt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(true)
  }

  it should "return false when new transformation is default and current is not default" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = QuantilesTransformation(orderedDataTypeTest.defaultQuantiles, orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(false)
  }

  it should "return true when quantiles are different and neither is default" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = QuantilesTransformation(IndexedSeq(4, 5, 6), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(true)
  }

  it should "return false when quantiles are the same and neither is default" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val newQt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    qt.isSupersededBy(newQt) should be(false)
  }

  "merge" should "return other transformation when it is a QuantileTransformation" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val otherQt = QuantilesTransformation(IndexedSeq(4, 5, 6), orderedDataTypeTest)
    qt.merge(otherQt) should be(otherQt)
  }

  it should "return current transformation when other is not a QuantileTransformation" in {
    val qt = QuantilesTransformation(IndexedSeq(1, 2, 3), orderedDataTypeTest)
    val other = new LinearTransformation(1, 10, 7, orderedDataTypeTest)
    qt.merge(other) should be(qt)
  }

}
