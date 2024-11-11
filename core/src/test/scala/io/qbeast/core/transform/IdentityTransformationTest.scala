package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

class IdentityTransformationTest extends AnyFlatSpec with Matchers {
  behavior of "IdentityTransformation"

  it should "transform value correctly" in {
    val zeroId = IdentityTransformation(0, IntegerDataType)
    zeroId.transform(0) shouldBe 0.0
    zeroId.transform(null) shouldBe 0.0
    an[scala.MatchError] should be thrownBy zeroId.transform(1)
  }

  it should "be superseded by another IdentityTransformation" in {
    val nullId = IdentityTransformation(null, IntegerDataType)
    val zeroId = IdentityTransformation(0, IntegerDataType)
    val oneId = IdentityTransformation(1, IntegerDataType)
    // null should be superseded by non-null
    nullId.isSupersededBy(zeroId) shouldBe true
    // non-null should not be superseded by null
    zeroId.isSupersededBy(nullId) shouldBe false
    // non-null should not be superseded by non-null
    nullId.isSupersededBy(nullId) shouldBe false
    // different non-null should be superseded
    zeroId.isSupersededBy(oneId) shouldBe true
    oneId.isSupersededBy(zeroId) shouldBe true
  }

  it should "merge with another IdentityTransformation" in {
    val nullId = IdentityTransformation(null, IntegerDataType)
    val zeroId = IdentityTransformation(0, IntegerDataType)
    val fiveId = IdentityTransformation(5, IntegerDataType)
    // [null] + [non-null] => [non-null]
    nullId.merge(zeroId) shouldBe zeroId
    // [non-null] + [null] => [non-null]
    zeroId.merge(nullId) shouldBe zeroId
    // [non-null] + [non-null] => [non-null]
    nullId.merge(nullId) shouldBe nullId
    // [0] + [5] => [0, 5]
    zeroId.merge(fiveId) should matchPattern {
      case LinearTransformation(0, 5, _, IntegerDataType) =>
    }
    // [5] + [0] => [0, 5]
    fiveId.merge(zeroId) should matchPattern {
      case LinearTransformation(0, 5, _, IntegerDataType) =>
    }
  }

  it should "be superseded by a LinearTransformation" in {
    val linear = LinearTransformation(-10, 10, 0, IntegerDataType)
    IdentityTransformation(null, IntegerDataType).isSupersededBy(linear) shouldBe true
    IdentityTransformation(0, IntegerDataType).isSupersededBy(linear) shouldBe true
    IdentityTransformation(-20, IntegerDataType).isSupersededBy(linear) shouldBe true
    IdentityTransformation(20, IntegerDataType).isSupersededBy(linear) shouldBe true
  }

  it should "merge with a LinearTransformation" in {
    val linear = LinearTransformation(-10, 10, 5000, IntegerDataType)
    // [null] + [-10, 10] => [-10, 10]
    IdentityTransformation(null, IntegerDataType).merge(linear) should matchPattern {
      case LinearTransformation(-10, 10, _, IntegerDataType) =>
    }
    // [0] + [-10, 10] => [-10, 10]
    IdentityTransformation(0, IntegerDataType).merge(linear) should matchPattern {
      case LinearTransformation(-10, 10, _, IntegerDataType) =>
    }
    // [-20] + [-10, 10] => [-20, 10]
    IdentityTransformation(-20, IntegerDataType).merge(linear) should matchPattern {
      case LinearTransformation(-20, 10, _, IntegerDataType) =>
    }
    // [20] + [-10, 10] => [-10, 20]
    IdentityTransformation(20, IntegerDataType).merge(linear) should matchPattern {
      case LinearTransformation(-10, 20, _, IntegerDataType) =>
    }
  }

  "IdentityToZeroTransformation" should "be superseded correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val idToZero = IdentityToZeroTransformation(1)
      idToZero.isSupersededBy(idToZero) shouldBe false
      idToZero.isSupersededBy(IdentityToZeroTransformation(2)) shouldBe true
      idToZero.isSupersededBy(NullToZeroTransformation) shouldBe true
      idToZero.isSupersededBy(IdentityTransformation(1, IntegerDataType)) shouldBe true
      idToZero.isSupersededBy(LinearTransformation(-10, 10, 0, IntegerDataType)) shouldBe true
    }
    test()
  }

  it should "merge correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val idToZero = IdentityToZeroTransformation(1)
      val id = IdentityTransformation(1, IntegerDataType)
      val linear = LinearTransformation(-10, 10, 0, IntegerDataType)

      idToZero.merge(NullToZeroTransformation) shouldBe NullToZeroTransformation
      idToZero.merge(id) shouldBe id
      idToZero.merge(linear) shouldBe linear
    }
    test()
  }

  "NullToZeroTransformation" should "be superseded correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val nullToZero = NullToZeroTransformation
      val idToZero = IdentityToZeroTransformation(1)
      val id = IdentityTransformation(1, IntegerDataType)
      val linear = LinearTransformation(-10, 10, 0, IntegerDataType)

      nullToZero.isSupersededBy(nullToZero) shouldBe false
      nullToZero.isSupersededBy(idToZero) shouldBe true
      nullToZero.isSupersededBy(id) shouldBe true
      idToZero.isSupersededBy(linear) shouldBe true
    }
    test()
  }

  it should "merge correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val nullToZero = NullToZeroTransformation
      val idToZero = IdentityToZeroTransformation(1)
      val id = IdentityTransformation(1, IntegerDataType)
      val linear = LinearTransformation(-10, 10, 0, IntegerDataType)

      nullToZero.merge(nullToZero) shouldBe nullToZero
      nullToZero.merge(idToZero) shouldBe idToZero
      nullToZero.merge(id) shouldBe id
      idToZero.merge(linear) shouldBe linear
    }
    test()
  }

}
