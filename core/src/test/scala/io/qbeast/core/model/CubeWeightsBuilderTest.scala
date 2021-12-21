package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class CubeWeightsBuilderTest extends AnyFlatSpec with Matchers {

  private val point = Point(0.66, 0.28)
  private val numDimensions = point.coordinates.length
  private val List(root, id10, id1001) = CubeId.containers(point).take(3).toList

  case class CubeWeightTesting(cube: CubeId, normalizedWeight: NormalizedWeight)

  object CubeWeightTesting {

    def apply(cubeWeight: CubeNormalizedWeight): CubeWeightTesting =
      CubeWeightTesting(CubeId(numDimensions, cubeWeight.cubeBytes), cubeWeight.normalizedWeight)

  }

  class CubeWeightsBuilderTesting(
      desiredCubeSize: Int,
      groupSize: Int,
      bufferCapacity: Int,
      announcedOrReplicatedSet: Set[CubeId] = Set.empty)
      extends CubeWeightsBuilder(
        desiredCubeSize,
        groupSize,
        bufferCapacity,
        announcedOrReplicatedSet)

  "CubeWeightsBuilder" should "calculate maxWeight for the roots" in {
    val builder = new CubeWeightsBuilderTesting(10, 10, 100000)
    Random.shuffle(0.to(100).toList).foreach { value => builder.update(point, Weight(value)) }
    val weights = builder.result().map(CubeWeightTesting.apply)
    weights.find(_.cube.equals(root)).get.normalizedWeight shouldBe Weight(9).fraction
  }

  it should "give the same results whether the data is sorted or not" in {
    val randomBuilder = new CubeWeightsBuilderTesting(100, 100, 100000)
    val sortedBuilder = new CubeWeightsBuilderTesting(100, 100, 100000)

    Random.shuffle(0.to(1000).toList).foreach { value =>
      randomBuilder.update(point, Weight(value))
    }
    0.to(1000).foreach { value => sortedBuilder.update(point, Weight(value)) }
    randomBuilder.result().map(CubeWeightTesting.apply) shouldBe sortedBuilder
      .result()
      .map(CubeWeightTesting.apply)
  }

  it should "add weights to the cube until it is full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)
    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(4).fraction))
  }

  it should "assign a the correct normalized maxWeight if the cube is not full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)
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

  it should "move the biggest maxWeight to a child cube if the cube is full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)
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

  it should "add maxWeight to the child of announced cube" in {
    val builder =
      new CubeWeightsBuilderTesting(1, 1, 100000, announcedOrReplicatedSet = Set(root))
    builder.update(point, Weight(2))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(2).fraction))
  }

  it should "add maxWeight to the child of replicated cube" in {
    val builder =
      new CubeWeightsBuilderTesting(1, 1, 100000, announcedOrReplicatedSet = Set(root))
    builder.update(point, Weight(2))
    builder.result().map(CubeWeightTesting.apply) shouldBe Seq(
      CubeWeightTesting(root, Weight(2).fraction),
      CubeWeightTesting(id10, Weight(2).fraction))
  }

}
