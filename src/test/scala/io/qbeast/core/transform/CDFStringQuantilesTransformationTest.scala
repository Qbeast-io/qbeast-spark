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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable
import scala.util.Random

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

  it should "supersede correctly" in {
    val customT_1 =
      CDFStringQuantilesTransformation(Array("brand_A", "brand_B", "brand_C"))
    val customT_2 =
      CDFStringQuantilesTransformation(Array("brand_A", "brand_B", "brand_D"))

    customT_1.isSupersededBy(customT_1) shouldBe false
    customT_1.isSupersededBy(customT_2) shouldBe true
  }

  // TODO: check this test
  it should "have quantiles with length > 1" in {
    an[IllegalArgumentException] should be thrownBy
      CDFStringQuantilesTransformation(Array.empty[String])

    an[IllegalArgumentException] should be thrownBy
      CDFStringQuantilesTransformation(Array("a"))
  }

}
