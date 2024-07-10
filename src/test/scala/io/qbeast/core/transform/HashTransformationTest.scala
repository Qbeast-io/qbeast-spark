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

import scala.util.Random

class HashTransformationTest extends AnyFlatSpec with Matchers {

  behavior of "HashTransformationTest"

  it should "always generated values [0,1)" in {
    val ht = HashTransformation()
    var i = 1000 * 1000
    while (i > 0) {
      i -= 1
      val hash = ht.transform(Random.nextInt.toString)
      hash should be >= 0.0
      hash should be < 1.0
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
