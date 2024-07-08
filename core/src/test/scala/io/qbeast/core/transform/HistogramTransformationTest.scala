package io.qbeast.core.transform

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.FloatDataType
import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.LongDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HistogramTransformationTest extends AnyFlatSpec with Matchers {
  val intHistogram = IndexedSeq(1, 2, 3, 4, 5)
  val longHistogram = IndexedSeq[Long](1L, 2L, 3L, 4L, 5L)
  val floatHistogram = IndexedSeq[Float](1.0f, 2.0f, 3.0f, 4.0f, 5.0f)
  val doubleHistogram = IndexedSeq[Double](1.0, 2.0, 3.0, 4.0, 5.0)
  val intHistogramTransformation = HistogramTransformation(intHistogram, IntegerDataType)

  val longHistogramTransformation =
    HistogramTransformation(longHistogram, LongDataType)

  val floatHistogramTransformation =
    HistogramTransformation(floatHistogram, FloatDataType)

  val doubleHistogramTransformation =
    HistogramTransformation(doubleHistogram, DoubleDataType)

  "A HistogramTransformation" should "transform Int values correctly" in {
    intHistogramTransformation.transform(3) should be(0.5)
  }

  it should "transform Long values correctly" in {
    longHistogramTransformation.transform(3L) should be(0.5)
  }

  it should "transform Float values correctly" in {
    floatHistogramTransformation.transform(3.0f) should be(0.5)
  }

  it should "transform Double values correctly" in {
    doubleHistogramTransformation.transform(3.0) should be(0.5)
  }

  // Add more tests for HistogramTransformation
  val histogramTransformation = intHistogramTransformation

  it should "handle null values correctly" in {
    histogramTransformation.transform(null) should be(0.0)
  }

  it should "handle edge case values correctly" in {
    histogramTransformation.transform(0) should be(0.0)
    histogramTransformation.transform(6) should be(1.0)
  }

  it should "handle large input correctly" in {
    histogramTransformation.transform(Int.MaxValue) should be(1.0)
  }

  it should "handle small input correctly" in {
    histogramTransformation.transform(Int.MinValue) should be(0.0)
  }

  it should "handle negative input correctly" in {
    val negativeHistogram = IndexedSeq(-5, -4, -3, -2, -1)
    val negativeHistogramTransformation =
      HistogramTransformation(negativeHistogram, IntegerDataType)
    negativeHistogramTransformation.transform(-3) should be(0.5)
  }

  it should "handle positive input correctly" in {
    histogramTransformation.transform(3) should be(0.5)
  }

  it should "handle non-numeric input correctly" in {
    assertThrows[ClassCastException] {
      histogramTransformation.transform("non-numeric")
    }
  }

  it should "handle empty input correctly" in {
    val emptyHistogram = IndexedSeq()
    val emptyHistogramTransformation = HistogramTransformation(emptyHistogram, IntegerDataType)
    emptyHistogramTransformation.transform(1) shouldBe 0.0
  }

}
