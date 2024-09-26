/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.model

import io.qbeast.spark.QbeastIntegrationTestSpec

/**
 * Tests of NormalizedWeight.
 */
class NormalizedWeightTest extends QbeastIntegrationTestSpec {

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

    def testDistribution(unbalancedDistribution: List[Int], desiredSize: Int = 10): Unit = {
      val total = unbalancedDistribution.sum
      val idealLoad = desiredSize.toDouble / total
      unbalancedDistribution
        .map(NormalizedWeight(desiredSize, _))
        .reduce(NormalizedWeight.merge) shouldBe idealLoad +- (0.01 * idealLoad)
    }
    testDistribution(List(10, 10, 10, 2, 1, 10, 10, 1))
    testDistribution(List(10, 10, 10, 2, 1, 10, 10, 1), desiredSize = 1000)

    testDistribution(List(1000), desiredSize = 10)

    testDistribution(List(1000), desiredSize = 10000)
    testDistribution(List(1000, 1), desiredSize = 100)

  }

  it should "compute NormalizedWeight from MaxWeight Column" in withSpark(spark => {
    import spark.implicits._
    val maxWeightIntegers = Seq(1, 100, 200)
    val df = maxWeightIntegers.toDF("maxWeightInt")
    val result =
      df.select(NormalizedWeight.fromWeightColumn($"maxWeightInt")).as[NormalizedWeight].collect()
    result should contain theSameElementsAs maxWeightIntegers.map(Weight(_).fraction)
  })

  it should "compute NormalizedWeight from the desiredCubeSize and the actual cube size columns" in
    withSpark(spark => {
      import spark.implicits._
      val desiredCubeSize = 100
      val cubeSize = 10
      val df = Seq((desiredCubeSize, cubeSize)).toDF("desiredCubeSize", "cubeSize")
      val result = df
        .select(NormalizedWeight.fromColumns($"desiredCubeSize", $"cubeSize"))
        .as[Double]
        .collect()
      result should contain theSameElementsAs Seq(NormalizedWeight(desiredCubeSize, cubeSize))
    })

}
