/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{CubeId, CubeInfo, LocalTree}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.MissingCubeDomainEstimation._
import scala.collection.mutable

class MissingCubeDomainEstimationTest extends QbeastIntegrationTestSpec {
  // groupCubeSize: 100

  //              root(0.2,300)
  //               /         \
  //        c1(0.4,150)    c2(2, 50)
  //             /
  //        c3(2,50)

  val tree: mutable.Builder[(CubeId, CubeInfo), mutable.Map[CubeId, CubeInfo]] =
    mutable.Map.newBuilder[CubeId, CubeInfo]

  val partitionSize = 300d

  val root: CubeId = CubeId.root(2)
  val c1: CubeId = root.firstChild
  val c2: CubeId = c1.nextSibling.get
  val c3: CubeId = c1.firstChild

  tree += (root -> CubeInfo(0.2, partitionSize))
  tree += (c1 -> CubeInfo(0.4, 150))
  tree += (c2 -> CubeInfo(2d, 50))
  tree += (c3 -> CubeInfo(2d, 50))

  val localTree: LocalTree = tree.result().toMap
  val emptyTree: LocalTree = Map.empty[CubeId, CubeInfo]

  "findClosestAncestorBottomUp" should "return the correct ancestor cube" in {
    findClosestAncestorBottomUp(emptyTree, root) shouldBe None
    findClosestAncestorBottomUp(localTree, c3.firstChild) shouldBe Some(c3)
    findClosestAncestorBottomUp(localTree, c3.firstChild.firstChild) shouldBe Some(c3)
    findClosestAncestorBottomUp(localTree, c2.firstChild.firstChild) shouldBe Some(c2)

    // During optimization, the LocalTree can miss some upper levels but this should
    // not affect the correctness of the algorithm
    findClosestAncestorBottomUp(localTree - root, c3.firstChild) shouldBe Some(c3)
    findClosestAncestorBottomUp(localTree - root, c1) shouldBe None
  }

  "initialFractionAndDomain" should
    "estimate fraction and domain from the closest ancestor correctly" in {
      val c1TreeSize = localTree(c1).treeSize
      val c2TreeSize = localTree(c2).treeSize
      val c3TreeSize = localTree(c3).treeSize

      val (f0, d0) = initialFractionAndDomain(root, localTree)
      f0 shouldBe 1d / (c1TreeSize + c2TreeSize + 1d)
      d0 shouldBe 0d

      val (f1, d1) = initialFractionAndDomain(c1, localTree)
      f1 shouldBe 1d / (c3TreeSize + 1d)
      d1 shouldBe 0d

      val (f2, d2) = initialFractionAndDomain(c2.firstChild, localTree)
      f2 shouldBe 1d
      d2 shouldBe 0d
    }

  "parentFractionAndPayload" should
    "estimate fraction and payload from subsequent ancestors correctly" in {
      val c1TreeSize = localTree(c1).treeSize
      val c2TreeSize = localTree(c2).treeSize

      val (f1, cs1) = parentFractionAndPayload(c1, localTree)
      f1 shouldBe c1TreeSize / (c1TreeSize + c2TreeSize)
      cs1 shouldBe 100d

      val (f2, cs2) = parentFractionAndPayload(c2, localTree)
      f2 shouldBe c2TreeSize / (c1TreeSize + c2TreeSize)
      cs2 shouldBe 100d

      // Method on root should throw an exception
      an[NoSuchElementException] should be thrownBy parentFractionAndPayload(root, localTree)
    }

  "domainThroughPayloadFractions" should "estimate missing cube domain correctly" in {
    val missingCube = c3.nextSibling.get
    // c1
    val ca = findClosestAncestorBottomUp(localTree, missingCube).get
    // f and d from c1 should be 1d / 51d and 0d
    val (f0, _) = initialFractionAndDomain(ca, localTree)
    // f and d from root to c1 should be 150d / 200d and 100d
    val (f1, d1) = parentFractionAndPayload(ca, localTree)

    domainThroughPayloadFractions(missingCube, localTree) shouldBe f0 * f1 * d1

    // it should return 0d when applied on root
    domainThroughPayloadFractions(root, localTree) shouldBe 0d
  }
}
