/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.model.Point
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

/**
 * Tests of CubeWeights.
 */
class CubeWeightsTest extends AnyFlatSpec with Matchers {
  private val point = Point(0.66, 0.28)
  private val numDimensions = point.coordinates.length
  private val List(root, id10, id1001) = CubeId.containers(point).take(3).toList
  case class CubeWeightTesting(cube: CubeId, normalizedWeight: NormalizedWeight)

  object CubeWeightTesting {

    def apply(cubeWeight: CubeNormalizedWeight): CubeWeightTesting =
      CubeWeightTesting(CubeId(numDimensions, cubeWeight.cubeBytes), cubeWeight.normalizedWeight)

  }

  "findTargetCubeIds" should "return the root cube if cube weights is empty" in {
    val cubeIds = CubeWeights.findTargetCubeIds(point, Weight(1), Map.empty, Set.empty, Set.empty)
    cubeIds shouldBe Seq(root)
  }

  it should "return the first cube with correct weight" in {
    val cubeIds = CubeWeights.findTargetCubeIds(
      point,
      Weight(2),
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set.empty,
      Set.empty)
    cubeIds shouldBe Seq(id10)
  }

  it should "return the child of announced cube with correct weight" in {
    val cubeIds = CubeWeights.findTargetCubeIds(
      point,
      Weight(2),
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set(id10),
      Set(root))
    cubeIds shouldBe Seq(id10, id1001)
  }

  it should "return the child of replicated cube with correct weight" in {
    val cubeIds = CubeWeights.findTargetCubeIds(
      point,
      Weight(2),
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set.empty,
      Set(root, id10))
    cubeIds shouldBe Seq(id10, id1001)
  }

  it should "return the first child cube of the specified parent with correct weight" in {
    val cubeIds = CubeWeights.findTargetCubeIds(
      point,
      Weight(2),
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set.empty,
      Set.empty,
      Some(id10))
    cubeIds shouldBe Seq(id1001)
  }

  it should "return the child of announced parent cube with correct weight" in {
    val cubeIds = CubeWeights.findTargetCubeIds(
      point,
      Weight(2),
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set(id10),
      Set(root),
      Some(id10))
    cubeIds shouldBe Seq(id1001)
  }

  it should "return the child of replicated parent cube with correct weight" in {
    val cubeIds = CubeWeights.findTargetCubeIds(
      point,
      Weight(2),
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set.empty,
      Set(root, id10),
      Some(id10))
    cubeIds shouldBe Seq(id1001)
  }

  "merge" should "merge the cube weights correctly" in {
    val cubeWeights = Map(root -> Weight(1).fraction, id10 -> Weight(2).fraction)
    val estimatedCubeWeights = Map(root -> Weight(3).fraction, id1001 -> Weight(4).fraction)
    val mergedCubeWeights = CubeWeights.merge(cubeWeights, estimatedCubeWeights)
    val mergedRootWeight = (Weight(1) * Weight(3)) / (Weight(1) + Weight(3))

    mergedCubeWeights(
      root).value shouldBe mergedRootWeight.value +- 10 // there might be a small rounding error

    mergedCubeWeights(id10) shouldBe Weight(2)
    mergedCubeWeights(id1001) shouldBe Weight(4)
  }

  it should "return the cube weights as a merge with empty estimated cube weights" in {
    val cubeWeights = Map(root -> Weight(1).fraction, id10 -> Weight(2).fraction)
    val mergedCubeWeights = CubeWeights.merge(cubeWeights, Map.empty).mapValues(_.fraction)
    mergedCubeWeights shouldBe cubeWeights
  }

  it should "return the estimated cube weights as a merge with empty cube weights" in {
    val estimatedCubeWeights = Map(root -> Weight(3).fraction, id1001 -> Weight(4).fraction)
    val mergedCubeWeights =
      CubeWeights.merge(Map.empty, estimatedCubeWeights).mapValues(_.fraction)
    mergedCubeWeights shouldBe estimatedCubeWeights
  }

  it should "manage correctly multiple leaf  blocks" in {

    // if desired size is 1000
    // A = 333 elements, B = 500 => still a leaf
    CubeWeights.merge(Map(root -> 3.0), Map(root -> 2.0))(root) shouldBe Weight.MaxValue

    // A = 500 elements, B = 500 => full
    CubeWeights.merge(Map(root -> 2.0), Map(root -> 2.0))(root) shouldBe Weight.MaxValue

    // A =~ 666 elements, B = 500 => about 0.85
    CubeWeights.merge(Map(root -> 1.5), Map(root -> 2.0))(root).fraction shouldBe 0.85 +- 0.01

    // A > 1000 elements, w=0.1 (estimated 1000 elements), B = 10 => about 0.0999
    CubeWeights
      .merge(Map(root -> 0.1), Map(root -> 100.0))(root)
      .fraction shouldBe 0.0999 +- 0.001

    // A > 1000 elements, w=0.1 (estimated 1000 elements), B >1000, w=0.01  => about 0.009
    CubeWeights
      .merge(Map(root -> 0.1), Map(root -> 0.01))(root)
      .fraction shouldBe 0.009 +- 0.0001
  }

  "CubeWeightsBuilder" should "calculate weight for the roots" in {
    val builder = new CubeWeightsBuilder(10, 1)
    Random.shuffle(0.to(100).toList).foreach { value => builder.update(point, Weight(value)) }
    val weights = builder.result().map(CubeWeightTesting.apply)
    weights.find(_.cube.equals(root)).get.normalizedWeight shouldBe Weight(9).fraction
  }

  it should "give the same results whether the data is sorted or not" in {
    val randomBuilder = new CubeWeightsBuilder(100, 1)
    val sortedBuilder = new CubeWeightsBuilder(100, 1)

    Random.shuffle(0.to(1000).toList).foreach { value =>
      randomBuilder.update(point, Weight(value))
    }
    0.to(1000).foreach { value => sortedBuilder.update(point, Weight(value)) }
    randomBuilder.result().map(CubeWeightTesting.apply) shouldBe sortedBuilder
      .result()
      .map(CubeWeightTesting.apply)
  }

  it should "add weights to the cube until it is full" in {
    val builder = new CubeWeightsBuilder(2, 1)
    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(4).fraction))
  }

  it should "assign a the correct normalized weight if the cube is not full" in {
    val builder = new CubeWeightsBuilder(2, 1)
    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.update(point, Weight(5))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(4).fraction),
      CubeWeightTesting(id1001, 2.0))
  }

  it should "move the biggest weight to a child cube if the cube is full" in {
    val builder = new CubeWeightsBuilder(2, 1)
    builder.update(point, Weight(5))
    builder.update(point, Weight(6))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(4).fraction),
      CubeWeightTesting(id1001, Weight(6).fraction))
  }

  it should "add weight to the child of announced cube" in {
    val builder = new CubeWeightsBuilder(1, 1, announcedSet = Set(root))
    builder.update(point, Weight(2))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(2).fraction))
  }

  it should "add weight to the child of replicated cube" in {
    val builder = new CubeWeightsBuilder(1, 1, replicatedSet = Set(root))
    builder.update(point, Weight(2))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(2).fraction))
  }
}
