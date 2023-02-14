package io.qbeast.core.model

import io.qbeast.core.transform.{LinearTransformation, Transformation}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QuerySpaceFromToTest extends AnyFlatSpec with Matchers {

  private val toSpaceCoordinates
      : (Seq[Option[Any]], Seq[Transformation]) => Seq[Option[NormalizedWeight]] =
    (originalValues: Seq[Option[Any]], transformations: Seq[Transformation]) => {
      originalValues.zip(transformations).map {
        case (Some(f), transformation) => Some(transformation.transform(f))
        case _ => None
      }
    }

  "QuerySpaceFromTo" should "intersect correctly" in {
    val cube = CubeId.root(3)
    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "intersect correctly with non-existing coordinates" in {
    val cube = CubeId.root(2)
    val transformation = Seq(
      LinearTransformation(1, 2, IntegerDataType),
      LinearTransformation(1, 2, IntegerDataType))
    val from = toSpaceCoordinates(Seq(None, Some(0)), transformation)
    val to = toSpaceCoordinates(Seq(Some(2), None), transformation)
    val querySpaceFromTo = new QuerySpaceFromTo(from, to)

    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "exclude beyond left limit" in {
    val cube = CubeId.root(1)
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val from = toSpaceCoordinates(Seq(Some(-1)), transformation)
    val to = toSpaceCoordinates(Seq(Some(0)), transformation)
    val querySpaceFromTo = new QuerySpaceFromTo(from, to)

    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "exclude beyond right limit" in {
    val cube = CubeId.root(1)
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val from = toSpaceCoordinates(Seq(Some(3)), transformation)
    val to = toSpaceCoordinates(Seq(Some(4)), transformation)
    val querySpaceFromTo = new QuerySpaceFromTo(from, to)

    querySpaceFromTo.intersectsWith(cube) shouldBe false
  }

  it should "include the left limit" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(1))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the right SPACE limit" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(2))
    val to = Seq(Some(2))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the left limit and beyond" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(0))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include the right SPACE limit and beyond" in {
    val cube = CubeId.root(1)
    val from = Seq(Some(2))
    val to = Seq(Some(3))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "include query range to, though not really desired" in {
    // For the implementation of LessThanOrEqual
    val cube = CubeId.root(1)
    val from = Seq(Some(-1))
    val to = Seq(Some(1))
    val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
    val querySpaceFromTo = QuerySpace(from, to, transformation)

    querySpaceFromTo shouldBe a[QuerySpaceFromTo]
    querySpaceFromTo.intersectsWith(cube) shouldBe true
  }

  it should "throw error if dimensions size is different" in {
    val from = Seq(Some(1), Some(2), Some(3))
    val to = Seq(Some(2), Some(4), Some(5))
    val transformation = Seq(LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))

    a[AssertionError] shouldBe thrownBy(QuerySpace(from, to, transformation))
  }

  it should "throw error if coordinates size is different" in {
    val from = Seq(Some(1))
    val to = Seq(Some(2), Some(4))
    val transformation = Seq(
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType),
      LinearTransformation(Int.MinValue, Int.MaxValue, IntegerDataType))
    a[AssertionError] shouldBe thrownBy(QuerySpace(from, to, transformation))
  }

  "QuerySpace" should
    "create an empty space when the query space is larger than the revision right limit" in {
      val cube = CubeId.root(3)
      val from = Seq(Some(3))
      val to = Seq(Some(4))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val emptySpace = QuerySpace(from, to, transformation)

      emptySpace shouldBe a[EmptySpace]
      emptySpace.intersectsWith(cube) shouldBe false
    }

  it should
    "create an empty space when the query space is smaller than the revision left limit" in {
      val from = Seq(Some(-1))
      val to = Seq(Some(0))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val emptySpace = QuerySpace(from, to, transformation)

      emptySpace shouldBe a[EmptySpace]
    }

  it should
    "create an AllSpace instance when the query space is identical to the revision space" in {
      val from = Seq(Some(1))
      val to = Seq(Some(2))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val allSpace = QuerySpace(from, to, transformation)

      allSpace shouldBe a[AllSpace]
    }

  it should
    "create an AllSpace instance when the query space contains the revision space" in {
      val cube = CubeId.root(1)
      val from = Seq(Some(-1))
      val to = Seq(Some(3))
      val transformation = Seq(LinearTransformation(1, 2, IntegerDataType))
      val allSpace = QuerySpace(from, to, transformation)

      allSpace shouldBe a[AllSpace]
      allSpace.intersectsWith(cube)
    }
}
