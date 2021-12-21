package io.qbeast.core.model

import org.scalatest.{AppendedClues, PrivateMethodTester}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

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
        CubeWeightsBuilder invokePrivate estimateGroupCubeSize(
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

          CubeWeightsBuilder invokePrivate estimateGroupCubeSize(
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
        CubeWeightsBuilder invokePrivate estimateGroupCubeSize(
          1000000,
          15,
          2000000L,
          100000L) shouldBe 50000.0
      }
    }
  }

}
