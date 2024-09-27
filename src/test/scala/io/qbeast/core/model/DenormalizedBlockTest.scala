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

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.core.transform.EmptyTransformer

import scala.collection.immutable.SortedMap
import scala.collection.immutable.SortedSet

class DenormalizedBlockTest extends QbeastIntegrationTestSpec {

  "DenormalizedBlockTest.isLeaf" should "return correctly determine the leaf nodes" in {
    //            r
    //           / \
    //        c1    c3
    //        /      \
    //      c2        c4
    val root = CubeId.root(2)
    val c1 = root.firstChild
    val c2 = c1.firstChild
    val c3 = c1.nextSibling.get
    val c4 = c3.firstChild
    val cubeStatuses = SortedSet(root, c1, c2, c3, c4)

    DenormalizedBlock.isLeaf(cubeStatuses)(root) shouldBe false
    DenormalizedBlock.isLeaf(cubeStatuses - root)(root) shouldBe false

    DenormalizedBlock.isLeaf(cubeStatuses)(c1) shouldBe false
    DenormalizedBlock.isLeaf(cubeStatuses - c1)(c1) shouldBe false

    DenormalizedBlock.isLeaf(cubeStatuses)(c2) shouldBe true
    DenormalizedBlock.isLeaf(cubeStatuses - c2)(c2) shouldBe true
    DenormalizedBlock.isLeaf(cubeStatuses)(c2.firstChild) shouldBe true

    DenormalizedBlock.isLeaf(cubeStatuses)(c3) shouldBe false
    DenormalizedBlock.isLeaf(cubeStatuses - c3)(c3) shouldBe false
    DenormalizedBlock.isLeaf(cubeStatuses)(c3.nextSibling.get) shouldBe true

    DenormalizedBlock.isLeaf(cubeStatuses)(c4) shouldBe true
    DenormalizedBlock.isLeaf(cubeStatuses - c4)(c4) shouldBe true
    DenormalizedBlock.isLeaf(cubeStatuses)(c4.firstChild) shouldBe true

  }

  "IndexMetrics.denormalizedBlocks" should "return the denormalized blocks" in withSpark {
    spark =>
      //       r
      //      /
      //    c1
      import spark.implicits._

      val root = CubeId.root(2)
      val rootB1 = Block("f1.parquet", root, Weight(0d), Weight(0.5), 1, replicated = false)
      val rootB2 = Block("f2.parquet", root, Weight(0d), Weight(0.45), 1, replicated = false)
      val c1 = root.firstChild
      val c1B1 = Block("f1.parquet", c1, Weight(0.5), Weight(1.0), 1, replicated = false)
      val c1B2 = Block("f2.parquet", c1, Weight(0.45), Weight(1.0), 1, replicated = false)

      val cubeStatuses = SortedMap(
        root -> CubeStatus(root, Weight(1), NormalizedWeight(Weight(1)), Vector(rootB1, rootB2)),
        c1 -> CubeStatus(c1, Weight(1), NormalizedWeight(Weight(1)), Vector(c1B1, c1B2)))

      val t = EmptyTransformer("")
      val revision =
        Revision(
          1L,
          1L,
          QTableID(""),
          1,
          Vector(t),
          Vector(t.makeTransformation((_: String) => _)))

      val fileSize = 10L
      val indexFilesDs = Vector(
        IndexFile("f1.parquet", fileSize, 1, revision.revisionID, Vector(rootB1, c1B1)),
        IndexFile("f2.parquet", fileSize, 2, revision.revisionID, Vector(rootB2, c1B2))).toDS

      val denormalizedBlock =
        DenormalizedBlock.buildDataset(revision, cubeStatuses, indexFilesDs)

      denormalizedBlock.collect() should contain theSameElementsAs Vector(
        DenormalizedBlock(
          cubeId = root,
          isLeaf = false,
          filePath = "f1.parquet",
          revisionId = revision.revisionID,
          fileSize = fileSize,
          fileModificationTime = 1L,
          minWeight = Weight(0d),
          maxWeight = Weight(0.5),
          blockElementCount = 1L,
          blockReplicated = false),
        DenormalizedBlock(
          cubeId = root,
          isLeaf = false,
          filePath = "f2.parquet",
          revisionId = revision.revisionID,
          fileSize = fileSize,
          fileModificationTime = 2L,
          minWeight = Weight(0d),
          maxWeight = Weight(0.45),
          blockElementCount = 1L,
          blockReplicated = false),
        DenormalizedBlock(
          cubeId = c1,
          isLeaf = true,
          filePath = "f1.parquet",
          revisionId = revision.revisionID,
          fileSize = fileSize,
          fileModificationTime = 1L,
          minWeight = Weight(0.5),
          maxWeight = Weight(1.0),
          blockElementCount = 1L,
          blockReplicated = false),
        DenormalizedBlock(
          cubeId = c1,
          isLeaf = true,
          filePath = "f2.parquet",
          revisionId = revision.revisionID,
          fileSize = fileSize,
          fileModificationTime = 2L,
          minWeight = Weight(0.45),
          maxWeight = Weight(1.0),
          blockElementCount = 1L,
          blockReplicated = false))
  }

}
