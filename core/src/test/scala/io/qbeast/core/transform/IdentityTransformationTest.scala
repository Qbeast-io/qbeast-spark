package io.qbeast.core.transform

import io.qbeast.core.model.mapper
import io.qbeast.core.model.DoubleDataType
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
  }

  it should "serialize and deserialize correctly" in {
    val idZero = IdentityTransformation(0, IntegerDataType)
    val jsonZero = s"""{"className":"io.qbeast.core.transform.IdentityTransformation",
                  |"identityValue":0,
                  |"orderedDataType":"IntegerDataType"}""".stripMargin.replace("\n", "")
    mapper.writeValueAsString(idZero) shouldBe jsonZero
    mapper.readValue[IdentityTransformation](
      jsonZero,
      classOf[IdentityTransformation]) shouldBe idZero

    val idNull = IdentityTransformation(null, IntegerDataType)
    val jsonNull = s"""{"className":"io.qbeast.core.transform.IdentityTransformation",
                      |"identityValue":null,
                      |"orderedDataType":"IntegerDataType"}""".stripMargin.replace("\n", "")
    mapper.writeValueAsString(idNull) shouldBe jsonNull
    mapper.readValue[IdentityTransformation](
      jsonNull,
      classOf[IdentityTransformation]) shouldBe idNull
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

  it should "be superseded by other Transformations" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0.0, DoubleDataType)
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)

    idt.isSupersededBy(et) shouldBe false
    idt.isSupersededBy(ht) shouldBe true
    idt.isSupersededBy(cdf_nt) shouldBe true
  }

  it should "merge with other Transformations" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0d, DoubleDataType)
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)

    idt.merge(et) shouldBe idt
    idt.merge(ht) shouldBe ht
    idt.merge(cdf_nt) shouldBe cdf_nt
  }

  "IdentityToZeroTransformation" should "be superseded correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val idToZero = IdentityToZeroTransformation(1)
      val et = EmptyTransformation()
      val ht = HashTransformation()
      val idt = IdentityTransformation(1, IntegerDataType)
      val lt = LinearTransformation(-10, 10, 0, IntegerDataType)
      val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), IntegerDataType)

      idToZero.isSupersededBy(idToZero) shouldBe false
      idToZero.isSupersededBy(IdentityToZeroTransformation(2)) shouldBe true
      idToZero.isSupersededBy(NullToZeroTransformation) shouldBe true
      idToZero.isSupersededBy(et) shouldBe false
      idToZero.isSupersededBy(ht) shouldBe true
      idToZero.isSupersededBy(idt) shouldBe true
      idToZero.isSupersededBy(lt) shouldBe true
      idToZero.isSupersededBy(cdf_nt) shouldBe true
    }
    test()
  }

  it should "merge correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val idToZero = IdentityToZeroTransformation(1)
      val et = EmptyTransformation()
      val ht = HashTransformation()
      val idt = IdentityTransformation(1, IntegerDataType)
      val lt = LinearTransformation(-10, 10, 0, IntegerDataType)
      val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), IntegerDataType)

      idToZero.merge(NullToZeroTransformation) shouldBe NullToZeroTransformation
      idToZero.merge(et) shouldBe idToZero
      idToZero.merge(ht) shouldBe ht
      idToZero.merge(idt) shouldBe idt
      idToZero.merge(lt) shouldBe lt
      idToZero.merge(cdf_nt) shouldBe cdf_nt
    }
    test()
  }

  "NullToZeroTransformation" should "be superseded correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val nullToZero = NullToZeroTransformation
      val idToZero = IdentityToZeroTransformation(1)
      val et = EmptyTransformation()
      val ht = HashTransformation()
      val idt = IdentityTransformation(1, IntegerDataType)
      val lt = LinearTransformation(-10, 10, 0, IntegerDataType)
      val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), IntegerDataType)

      nullToZero.isSupersededBy(nullToZero) shouldBe false
      nullToZero.isSupersededBy(idToZero) shouldBe true
      nullToZero.isSupersededBy(et) shouldBe false
      nullToZero.isSupersededBy(ht) shouldBe true
      nullToZero.isSupersededBy(idt) shouldBe true
      nullToZero.isSupersededBy(lt) shouldBe true
      nullToZero.isSupersededBy(cdf_nt) shouldBe true
    }
    test()
  }

  it should "merge correctly" in {
    @nowarn("cat=deprecation") def test(): Unit = {
      val nullToZero = NullToZeroTransformation
      val idToZero = IdentityToZeroTransformation(1)
      val et = EmptyTransformation()
      val ht = HashTransformation()
      val idt = IdentityTransformation(1, IntegerDataType)
      val lt = LinearTransformation(-10, 10, 0, IntegerDataType)
      val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), IntegerDataType)

      nullToZero.merge(nullToZero) shouldBe nullToZero
      nullToZero.merge(idToZero) shouldBe idToZero
      nullToZero.merge(et) shouldBe nullToZero
      nullToZero.merge(ht) shouldBe ht
      nullToZero.merge(idt) shouldBe idt
      nullToZero.merge(lt) shouldBe lt
      nullToZero.merge(cdf_nt) shouldBe cdf_nt
    }
    test()
  }

}
