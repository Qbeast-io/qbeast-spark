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

import io.qbeast.core.model.StringDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable
import scala.util.Random

class CDFStringQuantilesTransformationTest extends AnyFlatSpec with Matchers {

  val defaultStringQuantiles: immutable.IndexedSeq[String] = (97 to 122).map(_.toChar.toString)

  "A CDFStringQuantilesTransformation" should "map values to [0d, 1d]" in {
    val attempts = 10
    val sht = CDFQuantilesTransformation(defaultStringQuantiles, StringDataType)
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
    val sht = CDFQuantilesTransformation(defaultStringQuantiles, StringDataType)
    val v = sht.transform(null)
    v should be <= 1d
    v should be >= 0d
  }

  it should "map existing values correctly" in {
    val quantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0).map(_.toString)
    val sht = CDFQuantilesTransformation(quantiles, StringDataType)
    quantiles.foreach(s => sht.transform(s) shouldBe s.toDouble)
  }

  it should "map non-existing values correctly" in {
    val quantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0).map(_.toString)
    val sht = CDFQuantilesTransformation(quantiles, StringDataType)
    sht.transform((quantiles.head.toDouble - 1).toString) shouldBe 0d
    sht.transform((quantiles.last.toDouble + 1).toString) shouldBe 1d

    quantiles.foreach { s =>
      val v = (s.toDouble + 0.1).toString
      sht.transform(v) shouldBe s.toDouble
    }
  }

  it should "supersede correctly" in {
    val defaultT = CDFQuantilesTransformation(defaultStringQuantiles, StringDataType)
    val customT_1 =
      CDFQuantilesTransformation(Array("brand_A", "brand_B", "brand_C"), StringDataType)
    val customT_2 =
      CDFQuantilesTransformation(Array("brand_A", "brand_B", "brand_D"), StringDataType)

    defaultT.isSupersededBy(customT_1) shouldBe true
    defaultT.isSupersededBy(defaultT) shouldBe false

    customT_1.isSupersededBy(defaultT) shouldBe false
    customT_1.isSupersededBy(customT_1) shouldBe false
    customT_1.isSupersededBy(customT_2) shouldBe true
  }

  it should "have histograms with length > 1" in {
    an[IllegalArgumentException] should be thrownBy
      CDFQuantilesTransformation(Array.empty[String], StringDataType)

    an[IllegalArgumentException] should be thrownBy
      CDFQuantilesTransformation(Array("a"), StringDataType)
  }

}
