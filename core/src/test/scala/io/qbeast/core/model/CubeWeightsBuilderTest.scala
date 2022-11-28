package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random
import scala.collection.mutable

class CubeWeightsBuilderTest extends AnyFlatSpec with Matchers {

  private val point = Point(0.66, 0.28)
  private val List(root, id10, id1001) = CubeId.containers(point).take(3).toList

  case class CubeAndInfoTesting(cube: CubeId, info: CubeInfo)

  class CubeWeightsBuilderTesting(
      desiredCubeSize: Int,
      groupSize: Int,
      bufferCapacity: Int,
      announcedOrReplicatedSet: Set[CubeId] = Set.empty)
      extends CubeWeightsBuilder(
        desiredCubeSize,
        groupSize,
        bufferCapacity,
        announcedOrReplicatedSet)

  "CubeWeightsBuilder" should "calculate maxWeight for the roots" in {
    val builder = new CubeWeightsBuilderTesting(10, 10, 100000)

    Random.shuffle(0.to(100).toList).foreach { value => builder.update(point, Weight(value)) }

    val localTree = builder.result().head

    localTree(root).normalizedWeight shouldBe Weight(9).fraction
    localTree(root).treeSize shouldBe 101d
  }

  it should "give the same results whether the data is sorted or not" in {
    val randomBuilder = new CubeWeightsBuilderTesting(100, 100, 100000)
    val sortedBuilder = new CubeWeightsBuilderTesting(100, 100, 100000)

    Random.shuffle(0.to(1000).toList).foreach { value =>
      randomBuilder.update(point, Weight(value))
    }

    0.to(1000).foreach { value => sortedBuilder.update(point, Weight(value)) }

    val randomTree = randomBuilder.result().head
    val sortedTree = sortedBuilder.result().head

    randomTree == sortedTree shouldBe true
  }

  it should "add weights to the cube until it is full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)

    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))

    builder
      .result()
      .head
      .toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 4)),
      (id10, CubeInfo(Weight(4).fraction, 2)))
  }

  it should "assign a the correct normalized maxWeight if the cube is not full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)

    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.update(point, Weight(5))

    builder
      .result()
      .head
      .toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 5)),
      (id10, CubeInfo(Weight(4).fraction, 3)),
      (id1001, CubeInfo(2.0, 1)))
  }

  it should "move the biggest maxWeight to a child cube if the cube is full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)

    builder.update(point, Weight(5))
    builder.update(point, Weight(6))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.update(point, Weight(1))
    builder.update(point, Weight(2))

    builder
      .result()
      .head
      .toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 6)),
      (id10, CubeInfo(Weight(4).fraction, 4)),
      (id1001, CubeInfo(Weight(6).fraction, 2)))
  }

  it should "add maxWeight to the child of announced cube" in {
    val builder =
      new CubeWeightsBuilderTesting(1, 1, 100000, announcedOrReplicatedSet = Set(root))

    builder.update(point, Weight(2))

    builder
      .result()
      .head
      .toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 2)),
      (id10, CubeInfo(Weight(2).fraction, 1)))
  }

  it should "add maxWeight to the child of replicated cube" in {
    val builder =
      new CubeWeightsBuilderTesting(1, 1, 100000, announcedOrReplicatedSet = Set(root))

    builder.update(point, Weight(2))

    builder
      .result()
      .head
      .toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 2)),
      (id10, CubeInfo(Weight(2).fraction, 1)))
  }

  it should "not duplicate elements in result" in {
    val builder =
      new CubeWeightsBuilderTesting(1, 1, 1000, announcedOrReplicatedSet = Set(root))

    0.to(10000).map { _ => builder.update(point, Weight(Random.nextInt())) }

    builder
      .result()
      .foreach(tree => tree.toSeq.size shouldBe tree.toSeq.distinct.size)
  }

  "populateTreeSize" should "create correct tree size results" in {
    // Initial Tree:
    //                                 root(_, 50)
    //                                /            \
    //                        c1(_, 50)             c6(_, 50)
    //                       /        \            /         \
    //               c2(_,15)      c3(_,50)    c7(_,30)      c8(_,30)
    //                             /      \
    //                        c4(_, 1)   c5(_,15)

    val root = CubeId.root(3)
    root.children.take(2)
    val Seq(c1, c6) = root.children.take(2).toSeq
    val Seq(c2, c3) = c1.children.take(2).toSeq
    val Seq(c4, c5) = c3.children.take(2).toSeq
    val Seq(c7, c8) = c6.children.take(2).toSeq

    val initialTree =
      mutable.Map[CubeId, CubeInfo](
        root -> CubeInfo(-1d, 50d),
        c1 -> CubeInfo(-1d, 50d),
        c2 -> CubeInfo(-1d, 15d),
        c3 -> CubeInfo(-1d, 50d),
        c4 -> CubeInfo(-1d, 1d),
        c5 -> CubeInfo(-1d, 15d),
        c6 -> CubeInfo(-1d, 50d),
        c7 -> CubeInfo(-1d, 30d),
        c8 -> CubeInfo(-1d, 30d))

    val builder = new CubeWeightsBuilderTesting(50, 50, 100000)
    val populatedTree = builder.populateTreeSize(initialTree.clone())

    populatedTree.foreach { case (cube, info) =>
      val cubeSize = initialTree(cube).treeSize
      val subtreeSize = cube.children
        .filter(populatedTree.contains)
        .map(populatedTree(_).treeSize)
        .sum

      info.treeSize shouldBe cubeSize + subtreeSize
    }
  }

}
