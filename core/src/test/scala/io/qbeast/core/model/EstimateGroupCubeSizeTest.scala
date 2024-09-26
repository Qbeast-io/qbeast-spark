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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.AppendedClues
import org.scalatest.PrivateMethodTester

class EstimateGroupCubeSizeTest
    extends AnyWordSpec
    with Matchers
    with AppendedClues
    with PrivateMethodTester {

  // numGroups = MAX(numPartitions, (numElements / cubeWeightsBufferCapacity))
  // groupCubeSize = desiredCubeSize / numGroups
  "EstimateGroupCubeSize" when {
    "numElements < desiredCubeSize and numElements < cubeWeightsBufferCapacity" should {
      "create the correct number of groups" in {
        val estimateGroupCubeSize = PrivateMethod[Double]('estimateGroupCubeSize)
        CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
          1000000,
          1,
          10000L,
          100000L) shouldBe 1000000.0
      }
    }

    "numElements == desiredCubeSize and numElements > cubeWeightsBufferCapacity" should {
      "create the correct number of groups for different number of partitions" in {
        val numPartitions = Seq(1, 10, 20)
        val groupCubeSizes = Seq(100000.0, 100000.0, 50000.0)
        val estimateGroupCubeSize = PrivateMethod[Double]('estimateGroupCubeSize)

        for ((nP, gS) <- numPartitions zip groupCubeSizes) {

          CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
            1000000,
            nP,
            1000000L,
            100000L) shouldBe gS withClue ("Number of partitions: " + nP)
        }
      }
    }

    "numElements > desiredCubeSize and numElements > cubeWeightsBufferCapacity" should {
      "create the correct number of groups" in {
        val estimateGroupCubeSize = PrivateMethod[Double]('estimateGroupCubeSize)
        CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
          1000000,
          15,
          2000000L,
          100000L) shouldBe 50000.0
      }
    }
  }

}
