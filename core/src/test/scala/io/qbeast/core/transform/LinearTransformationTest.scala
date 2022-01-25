package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class LinearTransformationTest extends AnyFlatSpec with Matchers {

  behavior of "LinearTransformation"

  it should "always generated values between the range" in {
    val min = Int.MinValue
    val max = Int.MaxValue
    val linearT = LinearTransformation(0, 10000, Random.nextInt(), IntegerDataType)

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
      Random.nextInt(),
      IntegerDataType)
  }

  it should "save min and max values" in {
    val linearT = LinearTransformation(0, 10000, Random.nextInt(), IntegerDataType)
    linearT.minNumber should be(0)
    linearT.maxNumber should be(10000)
  }

  // TODO check merge
  it should "merge transformations correctly" in {
    val randomNullValue = Random.nextInt()
    val linearT = LinearTransformation(0, 10000, randomNullValue, IntegerDataType)

    linearT.merge(
      LinearTransformation(
        0,
        90000,
        randomNullValue,
        IntegerDataType)) shouldBe LinearTransformation(
      0,
      90000,
      randomNullValue,
      IntegerDataType)

    linearT.merge(
      LinearTransformation(
        -100,
        10000,
        randomNullValue,
        IntegerDataType)) shouldBe LinearTransformation(
      -100,
      10000,
      randomNullValue,
      IntegerDataType)

    linearT.merge(
      LinearTransformation(
        -100,
        90000,
        randomNullValue,
        IntegerDataType)) shouldBe LinearTransformation(
      -100,
      90000,
      randomNullValue,
      IntegerDataType)

    linearT.merge(LinearTransformation(6, 9, randomNullValue, IntegerDataType)) shouldBe linearT

  }

}
