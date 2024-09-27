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
package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.core.model._
import org.apache.spark.sql.Dataset

class IndexMetricsTest extends QbeastIntegrationTestSpec {

  "IndexMetrics.computeAverageFanout" should
    "return the right avg fanout from a Dataset[CubeId]" in withSpark { spark =>
      import spark.implicits._
      val root = CubeId.root(2)
      // root -> c1, c2, c3, c4
      val rootChildren = root.children.toSeq
      // c1 => c11, c12, c13
      val c1Children = rootChildren.head.children.take(3).toSeq
      // c4 => c41, c42
      val c4Children = rootChildren.last.children.take(2).toSeq
      val fanout = (rootChildren.size + c1Children.size + c4Children.size) / 3d
      val cubes = ((root +: rootChildren) ++ c1Children ++ c4Children).toDS
      cubes.transform(IndexMetrics.computeAverageFanout).first() shouldBe Some(fanout)
    }

  "IndexMetrics.computeMinHeight" should "return the right min height" in {
    val desiredCubeSize = 100
    val dimensionCount = 2
    an[AssertionError] shouldBe thrownBy(
      IndexMetrics.computeMinHeight(-1, desiredCubeSize, dimensionCount) shouldBe 0)
    IndexMetrics.computeMinHeight(0, desiredCubeSize, dimensionCount) shouldBe 0
    IndexMetrics.computeMinHeight(1, desiredCubeSize, dimensionCount) shouldBe 1
    IndexMetrics.computeMinHeight(desiredCubeSize, desiredCubeSize, dimensionCount) shouldBe 1
    IndexMetrics.computeMinHeight(desiredCubeSize + 1, desiredCubeSize, dimensionCount) shouldBe 2
    IndexMetrics.computeMinHeight(desiredCubeSize * 6, desiredCubeSize, dimensionCount) shouldBe 3
    IndexMetrics.computeMinHeight(
      desiredCubeSize * 21 + 1,
      desiredCubeSize,
      dimensionCount) shouldBe 4
  }

  "computeCubeStats" should "compute cube stats strings correctly" in withSpark { spark =>
    import spark.implicits._

    val dimensionCount = 2 // Example dimension count

    // Creating the root CubeId
    val rootCubeId = CubeId.root(dimensionCount)

    // Assuming the first child of the root can be determined and so on for simplicity
    // This part is hypothetical and might need adjustment based on actual CubeId usage
    val child1CubeId = rootCubeId.firstChild
    val child2CubeId = rootCubeId.children.drop(1).next()

    val denormalizedBlocks: Dataset[DenormalizedBlock] = Seq(
      DenormalizedBlock(
        rootCubeId,
        isLeaf = true,
        "path1",
        1,
        1000,
        23456789L,
        Weight.MaxValue,
        Weight.MaxValue,
        100,
        blockReplicated = false),
      DenormalizedBlock(
        rootCubeId,
        isLeaf = true,
        "path2",
        1,
        1500,
        23456789L,
        Weight.MaxValue,
        Weight.MaxValue,
        150,
        blockReplicated = false),
      DenormalizedBlock(
        child1CubeId,
        isLeaf = false,
        "path3",
        1,
        2000,
        23456789L,
        Weight.MaxValue,
        Weight.MaxValue,
        200,
        blockReplicated = true),
      DenormalizedBlock(
        child2CubeId,
        isLeaf = false,
        "path4",
        1,
        2500,
        23456789L,
        Weight.MaxValue,
        Weight.MaxValue,
        250,
        blockReplicated = true)).toDS()

    // Assuming computeCubeStats is accessible and correctly implemented
    val statsString = IndexMetrics.computeCubeStats(denormalizedBlocks)

    // Example assertion
    statsString should include("avgCubeElementCount")
    statsString should include("cubeCount")
    statsString should include("blockCount")
    statsString should include("cubeElementCountStddev")
    statsString should include("cubeElementCountQuartiles")
    statsString should include("avgWeight")

  }

}
