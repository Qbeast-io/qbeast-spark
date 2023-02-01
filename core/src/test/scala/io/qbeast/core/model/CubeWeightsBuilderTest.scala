package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class CubeWeightsBuilderTest extends AnyFlatSpec with Matchers {

  private val point = Point(0.9, 0.1)

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

  "CubeWeightsBuilder" should "calculate domain for the root" in {
    val builder = new CubeWeightsBuilderTesting(10, 10, 100000)

    Random.shuffle(0.to(100).toList).foreach { value => builder.update(point, Weight(value)) }

    val partitionCubeDomains =
      builder.result().map(cd => (CubeId(1, cd.cubeBytes), cd.domain)).toMap

    partitionCubeDomains(CubeId.root(1)) shouldBe 101
  }

}
