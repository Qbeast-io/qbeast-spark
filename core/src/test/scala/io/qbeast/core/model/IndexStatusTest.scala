package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.SortedMap

class IndexStatusTest extends AnyFlatSpec with Matchers {

  it should "compute existing cube domains correctly" in {
    // existing index: Cube(NormalizedWeight, [blockSize])
    //            root(0.1, [10])
    //             /        \
    //      c1(0.7, [10])   c2(1.0, [8])
    //           /
    //    c3(1.0, [2])
    val fileBuilder = (new IndexFileBuilder).setPath("mockPath")

    val root = CubeId.root(2)
    fileBuilder
      .beginBlock()
      .setCubeId(root)
      .setMinWeight(Weight(0))
      .setMaxWeight(Weight(0.1))
      .setElementCount(10L)
      .endBlock()

    val Seq(c1, c2) = root.children.take(2).toList
    fileBuilder
      .beginBlock()
      .setCubeId(c1)
      .setMinWeight(Weight(0.1))
      .setMaxWeight(Weight(0.7))
      .setElementCount(10L)
      .endBlock()

    fileBuilder
      .beginBlock()
      .setCubeId(c2)
      .setMinWeight(Weight(0.1))
      .setMaxWeight(Weight(0.99))
      .setElementCount(8L)
      .endBlock()

    val c3 = c1.children.next
    fileBuilder
      .beginBlock()
      .setCubeId(c3)
      .setMinWeight(Weight(0.7))
      .setMaxWeight(Weight(0.99))
      .setElementCount(2L)
      .endBlock()

    val blocks = fileBuilder.result().blocks
    val cubeStatusesBuilder = SortedMap.newBuilder[CubeId, CubeStatus]
    blocks
      .foreach { b =>
        val cs = CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil)
        cubeStatusesBuilder += b.cubeId -> cs
      }

    val indexStatus = IndexStatus(
      Revision.firstRevision(QTableID(""), 100, Vector.empty, Vector.empty),
      cubeStatusesBuilder.result())
    val domains = indexStatus.cubeDomains()
    domains(root) shouldBe 30d
    domains(c1) shouldBe domains(root) * (12d / 20)
    domains(c2) shouldBe domains(root) * (8d / 20)
    domains(c3) shouldBe domains(c1)
  }

}
