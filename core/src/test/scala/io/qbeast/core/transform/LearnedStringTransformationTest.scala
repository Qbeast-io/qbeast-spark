package io.qbeast.core.transform

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LearnedStringTransformationTest extends AnyFlatSpec with Matchers {
  "LearnedCDF" should "encode input strings correctly" in {
    val learnedCdf = DefaultLearnedCDF()
    val enc_a = learnedCdf.encodeString("a")
    enc_a.head shouldBe 'a'.toFloat
    enc_a.tail.forall(_ == learnedCdf.paddingValue)

    val enc_abc = learnedCdf.encodeString("abc")
    enc_abc.count(_ == -1f) shouldBe learnedCdf.maxEncodingLength - 3
    enc_abc.length shouldBe learnedCdf.maxEncodingLength
  }

  it should "predict string position according to their alphabetical ordering" in {
    val learnedCdf = DefaultLearnedCDF()
    val inputStrings = Seq("a", "b", "c", "d", "z")
    val orderedByPreds =
      inputStrings.map(c => (c, learnedCdf.predict(c, 0f, 1f))).sortBy(_._2).map(_._1)
    inputStrings should contain theSameElementsInOrderAs orderedByPreds
  }

  "LearnedStringTransformation" should "map strings to [0.0, 1.0]" in {
    val lst = LearnedStringTransformation("a", "c")
    lst.transform("a") shouldBe 0d +- 1e-8
    lst.transform("c") shouldBe 1d +- 1e-8

    val b_pred = lst.transform("b")
    b_pred shouldBe <(1d)
    b_pred shouldBe >(0d)
  }

  it should "determine if it's superseded correctly" in {
    val a_c_lst = LearnedStringTransformation("a", "c")
    val b_d_lst = LearnedStringTransformation("b", "d")
    val ba_bb_lst = LearnedStringTransformation("ba", "bb")

    a_c_lst.isSupersededBy(b_d_lst) shouldBe true
    a_c_lst.isSupersededBy(ba_bb_lst) shouldBe false
    a_c_lst.isSupersededBy(a_c_lst) shouldBe false
    a_c_lst.isSupersededBy(IdentityToZeroTransformation(0d)) shouldBe false
  }

  it should "merge correctly" in {
    val thisLst = LearnedStringTransformation("a", "c")
    val thatLst = LearnedStringTransformation("b", "d")
    thisLst.merge(thatLst) match {
      case LearnedStringTransformation(minString, maxString) =>
        minString shouldBe "a"
        maxString shouldBe "d"
    }

    thisLst.merge(IdentityToZeroTransformation(0d)) match {
      case LearnedStringTransformation(minString, _) =>
        minString shouldBe " "
    }
  }

}
