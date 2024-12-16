package io.qbeast.core.transform

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFNumericQuantilesTransformationTest extends AnyFlatSpec with Matchers {

  "CDFNumericQuantilesTransformer" should "return transform correctly" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1d, 2d, 3d), IntegerDataType)
    qt.transform(0) should be(0.0)
    qt.transform(1.5) should be(0.0)
    qt.transform(2) should be(0.5)
    qt.transform(4) should be(1.0)
  }

  it should "return correct transformation for insertion point in the bin" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 3, 5), IntegerDataType)
    // 2 is between 1 and 3, so it should be 0.25 (fraction = 2-1 / 3-1 = 0.5. -> 0 + fraction(0.5) / 2 = 0.25)
    qt.transform(2) should be(0.25)
  }

  it should "return correct transformation for insertion point in the bin with repeated values" in {
    val qt = CDFNumericQuantilesTransformation(IndexedSeq(1, 1, 3, 5), IntegerDataType)
    // 2 is between 1 and 3 so it should be 0.5
    qt.transform(2) should be(0.5)
  }

  it should "return correct transformation point for all values inside the bin" in {
    val quantiles = IndexedSeq(1, 1, 1, 1, 1, 1, 100, 100, 100).map(_.toDouble)
    val qt = CDFNumericQuantilesTransformation(quantiles, IntegerDataType)
    val maxIndexQuantiles = quantiles.size - 1
    val transformations = 2.to(99).map(qt.transform)
    // All transformations should be mapped between the index of 1 and 100.
    transformations.foreach { v =>
      v shouldBe >=(5 / maxIndexQuantiles.toDouble)
      v shouldBe <=(6 / maxIndexQuantiles.toDouble)
    }
    // The transformations should be sorted
    transformations.sorted should be(transformations)
  }

  it should "force quantiles to have more than 1 value" in {
    an[IllegalArgumentException] should be thrownBy CDFNumericQuantilesTransformation(
      Vector(1d),
      IntegerDataType)

    an[IllegalArgumentException] should be thrownBy CDFNumericQuantilesTransformation(
      Vector(),
      IntegerDataType)
  }

  it should "be superseded by another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0d, DoubleDataType)
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)
    val cdf_nt_2 = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.4), DoubleDataType)
    val cdf_nt_3 = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.4), IntegerDataType)

    cdf_nt.isSupersededBy(et) shouldBe false
    cdf_nt.isSupersededBy(ht) shouldBe true
    cdf_nt.isSupersededBy(idt) shouldBe true
    cdf_nt.isSupersededBy(lt) shouldBe true
    cdf_nt.isSupersededBy(cdf_st) shouldBe true
    cdf_nt.isSupersededBy(cdf_nt) shouldBe false
    cdf_nt.isSupersededBy(cdf_nt_2) shouldBe true
    cdf_nt.isSupersededBy(cdf_nt_3) shouldBe true
  }

  it should "merge with another Transformation" in {
    val et = EmptyTransformation()
    val ht = HashTransformation()
    val idt = IdentityTransformation(0d, DoubleDataType)
    val lt = LinearTransformation(-100d, 100d, DoubleDataType)
    val cdf_st = CDFStringQuantilesTransformation(Vector("a", "b", "c"))
    val cdf_nt = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.3), DoubleDataType)
    val cdf_nt_2 = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.4), DoubleDataType)
    val cdf_nt_3 = CDFNumericQuantilesTransformation(Vector(0.1, 0.2, 0.4), IntegerDataType)

    cdf_nt.merge(et) shouldBe cdf_nt
    cdf_nt.merge(ht) shouldBe ht
    cdf_nt.merge(idt) shouldBe idt
    cdf_nt.merge(lt) shouldBe lt
    cdf_nt.merge(cdf_st) shouldBe cdf_st
    cdf_nt.merge(cdf_nt) shouldBe cdf_nt
    cdf_nt.merge(cdf_nt_2) shouldBe cdf_nt_2
    cdf_nt.merge(cdf_nt_3) shouldBe cdf_nt_3
  }

}
