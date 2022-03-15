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

}
