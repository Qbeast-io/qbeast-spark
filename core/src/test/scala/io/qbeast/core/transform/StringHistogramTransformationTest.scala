package io.qbeast.core.transform

import io.qbeast.core.transform.HistogramTransformer.defaultStringHistogram
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.util.Random

@nowarn("cat=deprecation")
class StringHistogramTransformationTest extends AnyFlatSpec with Matchers {

  "A StringHistogramTransformation" should "map values to [0d, 1d]" in {
    val attempts = 10
    val sht = StringHistogramTransformation(defaultStringHistogram)
    val minAsciiEnc = 32 // SPACE
    val maxAsciiEnc = 126 // ~
    (1 to attempts).foreach { _ =>
      val inputStr = (1 to 5)
        .map { _ =>
          val asciiEnc = minAsciiEnc + Random.nextInt(maxAsciiEnc - minAsciiEnc)
          asciiEnc.toChar.toString
        }
        .mkString("")
      val v = sht.transform(inputStr)

      v should be <= 1d
      v should be >= 0d
    }
  }

  it should "properly handle null" in {
    val sht = StringHistogramTransformation(defaultStringHistogram)
    val v = sht.transform(null)
    v should be <= 1d
    v should be >= 0d
  }

  it should "map existing values correctly" in {
    val hist = Array(0.0, 0.25, 0.5, 0.75, 1.0).map(_.toString)
    val sht = StringHistogramTransformation(hist)
    hist.foreach(s => sht.transform(s) shouldBe s.toDouble)
  }

  it should "map non-existing values correctly" in {
    val hist = Array(0.0, 0.25, 0.5, 0.75, 1.0).map(_.toString)
    val sht = StringHistogramTransformation(hist)
    sht.transform((hist.head.toDouble - 1).toString) shouldBe 0d
    sht.transform((hist.last.toDouble + 1).toString) shouldBe 1d

    hist.foreach { s =>
      val v = (s.toDouble + 0.1).toString
      sht.transform(v) shouldBe s.toDouble
    }
  }

  it should "be superseded by another StringHistogramTransformation" in {
    val defaultT = StringHistogramTransformation(defaultStringHistogram)
    val customT_1 =
      StringHistogramTransformation(Array("brand_A", "brand_B", "brand_C"))
    val customT_2 =
      StringHistogramTransformation(Array("brand_A", "brand_B", "brand_D"))

    defaultT.isSupersededBy(customT_1) shouldBe true
    defaultT.isSupersededBy(defaultT) shouldBe false

    customT_1.isSupersededBy(defaultT) shouldBe false
    customT_1.isSupersededBy(customT_1) shouldBe false
    customT_1.isSupersededBy(customT_2) shouldBe true
  }

  it should "be superseded by other Transformations" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val sht = StringHistogramTransformation(defaultStringHistogram)

    sht.isSupersededBy(et) shouldBe false
    sht.isSupersededBy(ht) shouldBe true
    sht.isSupersededBy(cdf_st) shouldBe true
  }

  it should "merge with other Transformations" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val sht = StringHistogramTransformation(defaultStringHistogram)

    sht.merge(et) shouldBe sht
    sht.merge(ht) shouldBe ht
    sht.merge(cdf_st) shouldBe cdf_st
  }

  it should "have histograms with length > 1" in {
    an[IllegalArgumentException] should be thrownBy
      StringHistogramTransformation(Array.empty[String])

    an[IllegalArgumentException] should be thrownBy
      StringHistogramTransformation(Array("a"))
  }

}
