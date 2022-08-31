package io.qbeast.core.model

import io.qbeast.core.transform.LinearTransformation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QuerySpaceFromToTest extends AnyFlatSpec with Matchers {

  "QuerySpace from to" should "intersect correctly" in {

    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "not intersect with query space larger than the max value" in {
    val from = Seq(Some(3))
    val to = Seq(Some(4))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "not intersect with query space smaller than the min value" in {
    val from = Seq(Some(-1))
    val to = Seq(Some(0))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "include the right limit" in {
    val from = Seq(Some(2))
    val to = Seq(Some(2))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the right limit and beyond" in {
    val from = Seq(Some(2))
    val to = Seq(Some(3))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the left limit" in {
    val from = Seq(Some(1))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "exclude the left limit and beyond" in {
    val from = Seq(Some(-1))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpaceFromTo(from, to, transformation)

    val cube = CubeId.root(3)
    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "throw error if dimensions size is different" in {
    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    a[AssertionError] shouldBe thrownBy(QuerySpaceFromTo(from, to, transformation))
  }

  it should "throw error if coordinates size is different" in {
    val from = Seq(Some(1))
    val to = Seq(Some(2), Some(4))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    a[AssertionError] shouldBe thrownBy(QuerySpaceFromTo(from, to, transformation))
  }
}
