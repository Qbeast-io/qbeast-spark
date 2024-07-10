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

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LinearTransformationTest extends AnyFlatSpec with Matchers {

  behavior of "LinearTransformation"

  it should "always generated values between the range" in {
    val min = Int.MinValue
    val max = Int.MaxValue
    val linearT = LinearTransformation(0, 10000, 5000, IntegerDataType)

    var i = 100000
    while (i > 0) {
      i -= 1
      val t = linearT.transform(i)
      t should be >= (min.toDouble)
      t should be <= (max.toDouble)

    }
  }

  it should "throw exception when min is > max" in {
    an[IllegalArgumentException] should be thrownBy LinearTransformation(
      10000,
      0,
      5000,
      IntegerDataType)
  }

  it should "save min and max values" in {
    val linearT = LinearTransformation(0, 10000, 5000, IntegerDataType)
    linearT.minNumber should be(0)
    linearT.maxNumber should be(10000)
  }

  it should "create null value between min and max" in {
    val linearT = LinearTransformation(0, 10000, IntegerDataType)
    linearT.nullValue.asInstanceOf[Int] should be > (0)
    linearT.nullValue.asInstanceOf[Int] should be <= (10000)
  }

  it should "merge transformations correctly" in {
    val nullValue = 5000
    val linearT = LinearTransformation(0, 10000, nullValue, IntegerDataType)

    linearT.merge(
      LinearTransformation(0, 90000, 70725, IntegerDataType)) shouldBe LinearTransformation(
      0,
      90000,
      70725,
      IntegerDataType)

    linearT.merge(
      LinearTransformation(-100, 10000, 600, IntegerDataType)) shouldBe LinearTransformation(
      -100,
      10000,
      600,
      IntegerDataType)

    linearT.merge(
      LinearTransformation(-100, 90000, 57890, IntegerDataType)) shouldBe LinearTransformation(
      -100,
      90000,
      57890,
      IntegerDataType)

    linearT.merge(LinearTransformation(6, 9, 7, IntegerDataType)) shouldBe LinearTransformation(
      0,
      10000,
      7,
      IntegerDataType)

  }

  it should "merge IdentityTransformations correctly" in {
    val nullValue = 5000
    val linearT = LinearTransformation(0, 10000, nullValue, IntegerDataType)

    var otherNullValue =
      LinearTransformationUtils.generateRandomNumber(0, 90000, Option(42.toLong))
    linearT.merge(IdentityToZeroTransformation(90000)) shouldBe LinearTransformation(
      0,
      90000,
      otherNullValue,
      IntegerDataType)

    otherNullValue =
      LinearTransformationUtils.generateRandomNumber(-100, 10000, Option(42.toLong))
    linearT.merge(IdentityToZeroTransformation(-100)) shouldBe LinearTransformation(
      -100,
      10000,
      otherNullValue,
      IntegerDataType)

    otherNullValue = LinearTransformationUtils.generateRandomNumber(0, 10000, Option(42.toLong))
    linearT.merge(IdentityToZeroTransformation(10)) shouldBe LinearTransformation(
      0,
      10000,
      otherNullValue,
      IntegerDataType)
  }

  it should "detect new transformations that superseed" in {
    val nullValue = 5000
    val linearT = LinearTransformation(0, 10000, nullValue, IntegerDataType)

    linearT.isSupersededBy(IdentityToZeroTransformation(90000)) shouldBe true

    linearT.isSupersededBy(IdentityToZeroTransformation(-100)) shouldBe true

    linearT.isSupersededBy(IdentityToZeroTransformation(10)) shouldBe false

  }

}
