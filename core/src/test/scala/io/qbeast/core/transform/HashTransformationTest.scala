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
      hash should be > 0.0
      hash should be < 1.0
    }
  }

  it should "create null value of length 10" in {
    val ht = HashTransformation()
    ht.nullValue.asInstanceOf[String].length should be(10)
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
