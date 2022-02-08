/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests of CubeWeights.
 */
class PointWeightIndexerTest extends AnyFlatSpec with Matchers {
  private val point = Point(0.66, 0.28)
  private val List(root, id10, id1001) = CubeId.containers(point).take(3).toList

  case class TableChangesTest(
      mapCubeWeights: Map[CubeId, Weight],
      announcedOrReplicatedSet: Set[CubeId])
      extends TableChanges {
    val isNewRevision: Boolean = false
    val isOptimizeOperation: Boolean = false
    val deltaReplicatedSet: Set[CubeId] = Set.empty

    val updatedRevision: Revision =
      Revision(-1L, System.currentTimeMillis(), QTableID(""), 0, Vector.empty, Vector.empty)

    val replicatedSet: Set[CubeId] = Set.empty
    val announcedSet: Set[CubeId] = Set.empty
    def cubeWeights(cubeId: CubeId): Option[Weight] = mapCubeWeights.get(cubeId)
    def cubeState(cubeId: CubeId): String = "FLOODED"
  }

  "findTargetCubeIds" should "return the root cube if cube weights is empty" in {
    val pwi = new PointWeightIndexer(TableChangesTest(Map.empty, Set.empty))
    val cubeIds = pwi.findTargetCubeIds(point, Weight(1))
    cubeIds shouldBe Seq(root)
  }

  it should "return the first cube with correct maxWeight" in {

    val tc =
      TableChangesTest(Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)), Set.empty)
    val pwi = new PointWeightIndexer(tc)
    val cubeIds = pwi.findTargetCubeIds(point, Weight(2))
    cubeIds shouldBe Seq(id10)
  }

  it should "return the child of announced cube with correct maxWeight" in {
    val tc = TableChangesTest(
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set(id10, root))
    val pwi = new PointWeightIndexer(tc)
    val cubeIds = pwi.findTargetCubeIds(point, Weight(2))
    cubeIds shouldBe Seq(id10, id1001)
  }

  it should "return the child of replicated cube with correct maxWeight" in {
    val tc = TableChangesTest(
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set(root, id10))
    val pwi = new PointWeightIndexer(tc)
    val cubeIds = pwi.findTargetCubeIds(point, Weight(2))
    cubeIds shouldBe Seq(id10, id1001)
  }

  it should "return the first child cube of the specified parent with correct maxWeight" in {
    val tc =
      TableChangesTest(Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)), Set.empty)
    val pwi = new PointWeightIndexer(tc)
    val cubeIds = pwi.findTargetCubeIds(point, Weight(2), Some(id10))
    cubeIds shouldBe Seq(id1001)
  }

  it should "return the child of announcedOrReplicated parent cube with correct maxWeight" in {
    val tc = TableChangesTest(
      Map(root -> Weight(1), id10 -> Weight(2), id1001 -> Weight(3)),
      Set(root, id10))
    val pwi = new PointWeightIndexer(tc)
    val cubeIds = pwi.findTargetCubeIds(point, Weight(2), Some(id10))
    cubeIds shouldBe Seq(id1001)
  }
}
