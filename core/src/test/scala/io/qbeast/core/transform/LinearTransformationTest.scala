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

import io.qbeast.core.model.mapper
import io.qbeast.core.model.DoubleDataType
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
      t should be >= min.toDouble
      t should be <= max.toDouble

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
    linearT.nullValue.asInstanceOf[Int] should be > 0
    linearT.nullValue.asInstanceOf[Int] should be <= 10000
  }

  it should "serialize and deserialize correctly" in {
    val className = "io.qbeast.core.transform.LinearTransformation"
    val minNumber = -10
    val maxNumber = 10
    val nullValue = 0
    val orderedDataType = IntegerDataType

    val linear = LinearTransformation(minNumber, maxNumber, nullValue, orderedDataType)
    val json = s"""{"className":"$className",
         |"minNumber":$minNumber,
         |"maxNumber":$maxNumber,
         |"nullValue":$nullValue,
         |"orderedDataType":"${orderedDataType.name}"}""".stripMargin.replace("\n", "")
    mapper.writeValueAsString(linear) shouldBe json
    mapper.readValue[LinearTransformation](json, classOf[LinearTransformation]) shouldBe linear
  }

  it should "be superseded by LinearTransformations" in {
    val linearT = LinearTransformation(-10, 10, 0, IntegerDataType)
    linearT.isSupersededBy(LinearTransformation(-10, 10, 0, IntegerDataType)) shouldBe false
    linearT.isSupersededBy(LinearTransformation(-20, 10, 0, IntegerDataType)) shouldBe true
    linearT.isSupersededBy(LinearTransformation(-10, 20, 0, IntegerDataType)) shouldBe true
  }

  it should "merge with LinearTransformations" in {
    val linearT = LinearTransformation(-10, 10, 0, IntegerDataType)
    // [-10, 10] + [-20, 0] => [-20, 10]
    linearT.merge(LinearTransformation(-20, 0, 0, IntegerDataType)) should matchPattern {
      case LinearTransformation(-20, 10, _, IntegerDataType) =>
    }
    // [-10, 10] + [0, 20] => [-10, 20]
    linearT.merge(LinearTransformation(0, 20, 0, IntegerDataType)) should matchPattern {
      case LinearTransformation(-10, 20, _, IntegerDataType) =>
    }
    // [-10, 10] + [-20, 20] => [-20, 20]
    linearT.merge(LinearTransformation(-20, 20, 0, IntegerDataType)) should matchPattern {
      case LinearTransformation(-20, 20, _, IntegerDataType) =>
    }
  }

  it should "be superseded by IdentityTransformations" in {
    val linearT = LinearTransformation(-10, 10, 0, IntegerDataType)
    linearT.isSupersededBy(IdentityTransformation(null, IntegerDataType)) shouldBe false
    linearT.isSupersededBy(IdentityTransformation(0, IntegerDataType)) shouldBe false
    linearT.isSupersededBy(IdentityTransformation(-20, IntegerDataType)) shouldBe true
    linearT.isSupersededBy(IdentityTransformation(20, IntegerDataType)) shouldBe true
  }

  it should "merge with IdentityTransformations" in {
    val linearT = LinearTransformation(-10, 10, 0, IntegerDataType)
    // [null] + [-10, 10] => [-10, 10]
    linearT.merge(IdentityTransformation(null, IntegerDataType)) should matchPattern {
      case LinearTransformation(-10, 10, _, IntegerDataType) =>
    }
    // [1] + [-10, 10] => [-10, 10]
    linearT.merge(IdentityTransformation(1, IntegerDataType)) should matchPattern {
      case LinearTransformation(-10, 10, _, IntegerDataType) =>
    }
    // [-20] + [-10, 10] => [-20, 10]
    linearT.merge(IdentityTransformation(-20, IntegerDataType)) should matchPattern {
      case LinearTransformation(-20, 10, _, IntegerDataType) =>
    }
    // [20] + [-10, 10] => [-10, 20]
    linearT.merge(IdentityTransformation(20, IntegerDataType)) should matchPattern {
      case LinearTransformation(-10, 20, _, IntegerDataType) =>
    }

  }

  it should "be superseded by other Transformations" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)

    lt.isSupersededBy(et) shouldBe false
    lt.isSupersededBy(ht) shouldBe true
    lt.isSupersededBy(cdf_st) shouldBe true
    lt.isSupersededBy(cdf_nt) shouldBe true
  }

  it should "merge with other Transformations" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)

    lt.merge(et) shouldBe lt
    lt.merge(ht) shouldBe ht
    lt.merge(cdf_st) shouldBe cdf_st
    lt.merge(cdf_nt) shouldBe cdf_nt
  }

}
