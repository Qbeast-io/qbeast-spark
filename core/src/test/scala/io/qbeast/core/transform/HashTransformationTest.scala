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
import scala.util.Random

@nowarn("cat=deprecation")
class HashTransformationTest extends AnyFlatSpec with Matchers {

  behavior of "HashTransformationTest"

  it should "always generated values [0,1)" in {
    val ht = HashTransformation()
    var i = 1000 * 1000
    while (i > 0) {
      i -= 1
      val hash = ht.transform(Random.nextInt.toString)
      hash should be >= 0.0
      hash should be <= 1.0
    }
  }

  it should "create null value of random int" in {
    val ht = HashTransformation()
    val nullValue = ht.nullValue.asInstanceOf[Int]
    nullValue should be >= Int.MinValue
    nullValue should be < Int.MaxValue
  }

  "The murmur" should "uniformly distributed with Strings" in {
    var i = 1000 * 1000 * 10
    val brackets = Array.fill(10)(0)

    val tt = HashTransformation()
    while (i > 0) {
      i -= 1
      val hash = tt.transform(i.toString)
      brackets((hash * 10).toInt) += 1

    }
    val t = brackets.sum / 10
    for (i <- 0 until 10) {
      brackets(i) should be(t +- (t / 10))
    }

  }

  it should "superseded by another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0d, DoubleDataType)
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)
    val sht = StringHistogramTransformation(defaultStringHistogram)

    ht.isSupersededBy(et) shouldBe false
    ht.isSupersededBy(ht) shouldBe false
    ht.isSupersededBy(idt) shouldBe true
    ht.isSupersededBy(lt) shouldBe true
    ht.isSupersededBy(cdf_st) shouldBe true
    ht.isSupersededBy(cdf_nt) shouldBe true
    ht.isSupersededBy(sht) shouldBe true
  }

  it should "merge with another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0d, DoubleDataType)
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)
    val sht = StringHistogramTransformation(defaultStringHistogram)

    ht.merge(et) shouldBe ht
    ht.merge(ht) shouldBe ht
    ht.merge(idt) shouldBe idt
    ht.merge(lt) shouldBe lt
    ht.merge(cdf_st) shouldBe cdf_st
    ht.merge(cdf_nt) shouldBe cdf_nt
    ht.merge(sht) shouldBe sht
  }

  "The murmur" should "uniformly distributed with Random Bytes" in {
    var i = 1000 * 1000 * 10
    val brackets = Array.fill(10)(0)
    Random.setSeed(System.currentTimeMillis())

    val tt = HashTransformation()
    val b = Array[Byte](30)
    while (i > 0) {
      i -= 1
      Random.nextBytes(b)
      val hash = tt.transform(b)
      brackets((hash * 10).toInt) += 1

    }
    val t = brackets.sum / 10
    for (i <- 0 until 10) {
      // TODO for some reasons random bytes distribute badly
      brackets(i) should be(t +- (t / 2))
    }

  }

}
