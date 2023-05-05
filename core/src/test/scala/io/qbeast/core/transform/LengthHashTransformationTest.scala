package io.qbeast.core.transform

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class LengthHashTransformationTest extends AnyFlatSpec with Matchers {

  behavior of "LengthHashTransformation"

  it should "always generated values [0,1)" in {
    val ht = LengthHashTransformation(encodingLength = 11)
    var i = 1000 * 1000
    while (i > 0) {
      i -= 1
      val hash = ht.transform(Random.nextInt.toString)
      hash should be >= 0.0
      hash should be < 1.0
    }
  }

  it should "create null value of random int" in {
    val ht = LengthHashTransformation(encodingLength = 11)
    val nullValue = ht.nullValue.asInstanceOf[Int]
    nullValue should be >= Int.MinValue
    nullValue should be < Int.MaxValue
  }

  // TODO it does not respect lexicographic order
  //  why?
  "The hash" should "respect lexicographic order" in {

    val strings = Seq("a", "abc", "aaaa", "bhgfasj", "lado", "jshdvkkmkmmk", "llal")

    val tt = LengthHashTransformation(encodingLength = 11)
    val transformedStrings = strings.map(s => (s, tt.transform(s)))
    val sortedStrings = strings.sorted
    val sortedTransformedStrings = transformedStrings.sortBy(_._2).map(_._1)

    sortedTransformedStrings shouldBe sortedStrings

  }
}
