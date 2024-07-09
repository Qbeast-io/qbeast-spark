package io.qbeast.spark.utils

import io.qbeast.core.model._
import io.qbeast.core.transform.EmptyTransformer
import io.qbeast.spark.QbeastIntegrationTestSpec

import scala.collection.immutable.SortedMap

class IndexMetricsTest extends QbeastIntegrationTestSpec {

  "IndexMetrics.isLeaf" should "return correctly determine the leaf nodes" in {
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
    val cs =
      CubeStatus(root, Weight(1), NormalizedWeight(Weight(1)), Vector.empty[Block])
    val cubeStatuses = SortedMap(root -> cs, c1 -> cs, c2 -> cs, c3 -> cs, c4 -> cs)

    IndexMetrics.isLeaf(cubeStatuses)(root) shouldBe false
    IndexMetrics.isLeaf(cubeStatuses - root)(root) shouldBe false

    IndexMetrics.isLeaf(cubeStatuses)(c1) shouldBe false
    IndexMetrics.isLeaf(cubeStatuses - c1)(c1) shouldBe false

    IndexMetrics.isLeaf(cubeStatuses)(c2) shouldBe true
    IndexMetrics.isLeaf(cubeStatuses - c2)(c2) shouldBe true
    IndexMetrics.isLeaf(cubeStatuses)(c2.firstChild) shouldBe true

    IndexMetrics.isLeaf(cubeStatuses)(c3) shouldBe false
    IndexMetrics.isLeaf(cubeStatuses - c3)(c3) shouldBe false
    IndexMetrics.isLeaf(cubeStatuses)(c3.nextSibling.get) shouldBe true

    IndexMetrics.isLeaf(cubeStatuses)(c4) shouldBe true
    IndexMetrics.isLeaf(cubeStatuses - c4)(c4) shouldBe true
    IndexMetrics.isLeaf(cubeStatuses)(c4.firstChild) shouldBe true

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
        IndexMetrics.denormalizedBlocks(revision, cubeStatuses, indexFilesDs)

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

  "IndexMathOps.computeMinHeight" should "return the right min height" in {
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

  "computeCubeStats" should "compute cube stats strings correctly" in {
    fail("Not implemented")
  }

}
