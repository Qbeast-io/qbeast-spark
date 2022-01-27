package io.qbeast.core.model

import io.qbeast.core.transform.LinearTransformation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class QuerySpaceFromToTest extends AnyFlatSpec with Matchers {

  "QuerySpace from to" should "intersect correctly" in {

    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, Random.nextInt(), IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, Random.nextInt(), IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, Random.nextInt(), IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "detect a wrong intersection" in {
    val from = Seq(Some(2))
    val to = Seq(Some(4))
    val transformation = Seq(LinearTransformation(0, 2, 1, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "throw error if dimensions size is different" in {
    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation =
      Seq(LinearTransformation(Int.MinValue, Int.MaxValue, Random.nextInt(), IntegerDataType))
    a[AssertionError] shouldBe thrownBy(QuerySpaceFromTo(from, to, transformation))
  }

  it should "throw error if coordinates size is different" in {
    val from = Seq(Some(1))
    val to = Seq(Some(2), Some(4))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, Random.nextInt(), IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, Random.nextInt(), IntegerDataType))
    a[AssertionError] shouldBe thrownBy(QuerySpaceFromTo(from, to, transformation))
  }
}
