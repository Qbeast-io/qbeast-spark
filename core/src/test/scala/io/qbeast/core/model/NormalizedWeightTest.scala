/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests of NormalizedWeight.
 */
class NormalizedWeightTest extends AnyFlatSpec with Matchers {

  "NormalizedWeight" should "support merge with other maxWeight" in {
    NormalizedWeight.merge(2.0, 3.0) shouldBe 1.2
    NormalizedWeight.merge(0.0, 1.0) shouldBe 1.0
    NormalizedWeight.merge(1.0, 0.0) shouldBe 1.0
    NormalizedWeight.merge(0.0, 0.0) shouldBe 0.0
  }

  it should "support conversion to Weight" in {
    NormalizedWeight.toWeight(2.0) shouldBe Weight.MaxValue
    NormalizedWeight.toWeight(0.5) shouldBe Weight(0.5)
  }

  it should "support creation from Weight" in {
    NormalizedWeight(Weight(1)) shouldBe Weight(1).fraction
  }

  it should "support creation from desired and actual cube sizes" in {
    NormalizedWeight(3, 2) shouldBe 1.5
  }

  it should "estimate correctly unbalanced distribution actual" in {

    def testDistrib(unbalancedDistribution: List[Int], desiredSize: Int = 10): Unit = {
      val total = unbalancedDistribution.sum
      val idealLoad = desiredSize.toDouble / total
      unbalancedDistribution
        .map(NormalizedWeight(desiredSize, _))
        .reduce(NormalizedWeight.merge) shouldBe idealLoad +- (0.01 * idealLoad)
    }
    testDistrib(List(10, 10, 10, 2, 1, 10, 10, 1))
    testDistrib(List(10, 10, 10, 2, 1, 10, 10, 1), desiredSize = 1000)

    testDistrib(List(1000), desiredSize = 10)

    testDistrib(List(1000), desiredSize = 10000)
    testDistrib(List(1000, 1), desiredSize = 100)

  }

}
