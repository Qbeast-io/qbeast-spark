/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.transform.HistogramTransformer.defaultStringHistogram
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.collection.immutable
import scala.util.Random

@nowarn("cat=deprecation")
class CDFStringQuantilesTransformationTest extends AnyFlatSpec with Matchers {

  val defaultStringQuantilesTest: immutable.IndexedSeq[String] =
    (97 to 122).map(_.toChar.toString)

  "A CDFStringQuantilesTransformation" should "map values to [0d, 1d]" in {
    val attempts = 10
    val sht = CDFStringQuantilesTransformation(defaultStringQuantilesTest)
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
    val sht = CDFStringQuantilesTransformation(defaultStringQuantilesTest)
    val v = sht.transform(null)
    v should be <= 1d
    v should be >= 0d
  }

  it should "map existing values correctly" in {
    val quantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0).map(_.toString)
    val sht = CDFStringQuantilesTransformation(quantiles)
    quantiles.foreach(s => sht.transform(s) shouldBe s.toDouble)
  }

  it should "map non-existing values correctly" in {
    val quantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0).map(_.toString)
    val sht = CDFStringQuantilesTransformation(quantiles)
    sht.transform((quantiles.head.toDouble - 1).toString) shouldBe 0d
    sht.transform((quantiles.last.toDouble + 1).toString) shouldBe 1d

    quantiles.foreach { s =>
      val v = (s.toDouble + 0.1).toString
      sht.transform(v) shouldBe s.toDouble
    }
  }

  it should "be superseded by another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0, DoubleDataType)
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_st_2 = CDFStringQuantilesTransformation(Vector("a", "b", "d"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)
    val sht = StringHistogramTransformation(defaultStringHistogram)

    cdf_st.isSupersededBy(et) shouldBe false
    cdf_st.isSupersededBy(ht) shouldBe true
    cdf_st.isSupersededBy(idt) shouldBe true
    cdf_st.isSupersededBy(lt) shouldBe true
    cdf_st.isSupersededBy(cdf_st) shouldBe false
    cdf_st.isSupersededBy(cdf_st_2) shouldBe true
    cdf_st.isSupersededBy(cdf_nt) shouldBe true
    cdf_st.isSupersededBy(sht) shouldBe true
  }

  it should "merge with another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0d, DoubleDataType)
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_st_2 = CDFStringQuantilesTransformation(Vector("a", "b", "d"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)
    val sht = StringHistogramTransformation(defaultStringHistogram)

    cdf_st.merge(et) shouldBe cdf_st
    cdf_st.merge(ht) shouldBe ht
    cdf_st.merge(idt) shouldBe idt
    cdf_st.merge(lt) shouldBe lt
    cdf_st.merge(cdf_st) shouldBe cdf_st
    cdf_st.merge(cdf_st_2) shouldBe cdf_st_2
    cdf_st.merge(cdf_nt) shouldBe cdf_nt
    cdf_st.merge(sht) shouldBe sht
  }

  // TODO: check this test
  it should "have quantiles with length > 1" in {
    an[IllegalArgumentException] should be thrownBy
      CDFStringQuantilesTransformation(Array.empty[String])

    an[IllegalArgumentException] should be thrownBy
      CDFStringQuantilesTransformation(Array("a"))
  }

}
