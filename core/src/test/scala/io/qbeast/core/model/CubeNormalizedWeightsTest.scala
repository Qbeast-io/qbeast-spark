package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Test for CubeNormalizedWeights
 */
class CubeNormalizedWeightsTest extends AnyFlatSpec with Matchers {

  private val point = Point(0.66, 0.28)
  private val List(root, id10, id1001) = CubeId.containers(point).take(3).toList
  case class CubeWeightTesting(cube: CubeId, normalizedWeight: NormalizedWeight)

  "merge" should "merge the cube weights correctly" in {
    val cubeWeights = Map(root -> Weight(1).fraction, id10 -> Weight(2).fraction)
    val estimatedCubeWeights = Map(root -> Weight(3).fraction, id1001 -> Weight(4).fraction)
    val mergedCubeWeights =
      CubeNormalizedWeights.mergeNormalizedWeights(cubeWeights, estimatedCubeWeights)
    val mergedRootWeight = (Weight(1) * Weight(3)) / (Weight(1) + Weight(3))

    mergedCubeWeights(
      root).value shouldBe mergedRootWeight.value +- 10 // there might be a small rounding error

    mergedCubeWeights(id10) shouldBe Weight(2)
    mergedCubeWeights(id1001) shouldBe Weight(4)
  }

  it should "return the cube weights as a merge with empty estimated cube weights" in {
    val cubeWeights = Map(root -> Weight(1).fraction, id10 -> Weight(2).fraction)
    val mergedCubeWeights =
      CubeNormalizedWeights.mergeNormalizedWeights(cubeWeights, Map.empty).mapValues(_.fraction)
    mergedCubeWeights shouldBe cubeWeights
  }

  it should "return the estimated cube weights as a merge with empty cube weights" in {
    val estimatedCubeWeights = Map(root -> Weight(3).fraction, id1001 -> Weight(4).fraction)
    val mergedCubeWeights =
      CubeNormalizedWeights
        .mergeNormalizedWeights(Map.empty, estimatedCubeWeights)
        .mapValues(_.fraction)
    mergedCubeWeights shouldBe estimatedCubeWeights
  }

  it should "manage correctly multiple leaf  blocks" in {

    // if desired size is 1000
    // A = 333 elements, B = 500 => still a leaf
    CubeNormalizedWeights.mergeNormalizedWeights(Map(root -> 3.0), Map(root -> 2.0))(
      root) shouldBe Weight.MaxValue

    // A = 500 elements, B = 500 => full
    CubeNormalizedWeights.mergeNormalizedWeights(Map(root -> 2.0), Map(root -> 2.0))(
      root) shouldBe Weight.MaxValue

    // A =~ 666 elements, B = 500 => about 0.85
    CubeNormalizedWeights
      .mergeNormalizedWeights(Map(root -> 1.5), Map(root -> 2.0))(root)
      .fraction shouldBe 0.85 +- 0.01

    // A > 1000 elements, w=0.1 (estimated 1000 elements), B = 10 => about 0.0999
    CubeNormalizedWeights
      .mergeNormalizedWeights(Map(root -> 0.1), Map(root -> 100.0))(root)
      .fraction shouldBe 0.0999 +- 0.001

    // A > 1000 elements, w=0.1 (estimated 1000 elements), B >1000, w=0.01  => about 0.009
    CubeNormalizedWeights
      .mergeNormalizedWeights(Map(root -> 0.1), Map(root -> 0.01))(root)
      .fraction shouldBe 0.009 +- 0.0001
  }

}
