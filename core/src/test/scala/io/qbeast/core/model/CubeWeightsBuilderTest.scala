package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random
import scala.collection.mutable

class CubeWeightsBuilderTest extends AnyFlatSpec with Matchers {

  private val point = Point(0.9, 0.1)
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

  "CubeWeightsBuilder" should "calculate maxWeight for the root" in {
    val builder = new CubeWeightsBuilderTesting(10, 10, 100000)

    Random.shuffle(0.to(100).toList).foreach { value => builder.update(point, Weight(value)) }

    val localTree = builder.result().head

    val levelOne = localTree.filter(kv => kv._1.depth == 1)
    val rootWeight = levelOne.head._2.parentWeight
    rootWeight shouldBe Weight(9).fraction

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

  it should "respect groupCubeSize" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)

    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))

    val unpopulatedLocalTree = builder.resultInternal()
    unpopulatedLocalTree.toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 2)),
      (id10, CubeInfo(Weight(4).fraction, 2)))
  }

  it should "assign the correct NormalizedWeight if the cube is not full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)

    builder.update(point, Weight(1))
    builder.update(point, Weight(2))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.update(point, Weight(5))

    val localTree = builder.resultInternal()
    localTree.toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 2)),
      (id10, CubeInfo(Weight(4).fraction, 2)),
      (id1001, CubeInfo(NormalizedWeight(2, 1), 1)))
  }

  it should "move the biggest maxWeight to a child cube if the cube is full" in {
    val builder = new CubeWeightsBuilderTesting(2, 2, 100000)

    builder.update(point, Weight(5))
    builder.update(point, Weight(6))
    builder.update(point, Weight(3))
    builder.update(point, Weight(4))
    builder.update(point, Weight(1))
    builder.update(point, Weight(2))

    val localTree = builder.resultInternal()
    localTree.toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 2)),
      (id10, CubeInfo(Weight(4).fraction, 2)),
      (id1001, CubeInfo(Weight(6).fraction, 2)))
  }

  it should "add maxWeight to the child of announced or replicated cube" in {
    val builder =
      new CubeWeightsBuilderTesting(1, 1, 100000, announcedOrReplicatedSet = Set(root))

    builder.update(point, Weight(2))

    val localTree = builder.resultInternal()
    localTree.toSeq shouldBe Seq(
      (root, CubeInfo(Weight(2).fraction, 1)),
      (id10, CubeInfo(Weight(2).fraction, 1)))
  }

  "populateTreeSizeAndParentWeight" should "create correct tree size results" in {
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

    val cubeSizes = Map(
      root -> 50d,
      c1 -> 50d,
      c2 -> 15d,
      c3 -> 50d,
      c4 -> 1d,
      c5 -> 15d,
      c6 -> 50d,
      c7 -> 30d,
      c8 -> 30d)

    val unpopulatedTree = mutable.Map.empty[CubeId, CubeInfo]
    cubeSizes.foreach { case (cube, count) =>
      unpopulatedTree += cube -> CubeInfo(-1, count)
    }

    val builder = new CubeWeightsBuilderTesting(50, 50, 100000)
    val populatedTree = builder.populateTreeSizeAndParentWeight(unpopulatedTree.result())

    populatedTree.foreach { case (cube, info) =>
      val cubeSize = cubeSizes(cube)
      val subtreeSize = cube.children
        .filter(populatedTree.contains)
        .map(populatedTree(_).treeSize)
        .sum

      info.treeSize shouldBe cubeSize + subtreeSize
    }
  }

}
