package io.qbeast.core.model

import io.qbeast.core.model.Weight.MaxValue
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WeightAndCountTest extends AnyFlatSpec with Matchers with PrivateMethodTester {
  "WeightAndCountFactory" should "create proper instances of WeightAndCount" in {
    // Existing index:
    //        root(0.8)
    //          /
    //       c1(1.0)
    val root = CubeId.root(2)
    val l1Cubes = root.children
    val c1 = l1Cubes.next()
    val newCube = l1Cubes.next()

    val wcFactory = new WeightAndCountFactory(Map(root -> 0.8, c1 -> 1.0), 300)
    val root_ = wcFactory.createWeightAndCount(root)
    root_ shouldBe an[InnerCubeWeightAndCount]
    root_.weight shouldBe Weight(0.8)
    root_.count shouldBe 0

    val c1_ = wcFactory.createWeightAndCount(c1)
    c1_ shouldBe an[LeafCubeWeightAndCount]
    c1_.weight shouldBe MaxValue
    c1_.count shouldBe 300 - 1

    val newCube_ = wcFactory.createWeightAndCount(newCube)
    newCube_ shouldBe an[WeightAndCount]
    newCube_.weight shouldBe MaxValue
    newCube_.count shouldBe 0
  }

  "A new WeightAndCount" should "include, update, and convert correctly (when full)" in {
    val newWc = new WeightAndCount(MaxValue, 0)
    val groupCubeSize = 5
    val baseWeight = 0.54

    // Weights from Weight(0.55) to Weight(0.64)
    (1 to 10).foreach { i =>
      val w = Weight(baseWeight + (i.toDouble / 100))
      if (newWc.shouldInclude(w, groupCubeSize)) newWc.update(w)
    }
    val wts = newWc.toWeightAndTreeSize(groupCubeSize)

    // With the groupCubeSize being 5, it should only accept the first 5 records
    newWc.count shouldBe 5
    newWc.cubeSize shouldBe 5
    wts.treeSize shouldBe 5

    // A full new WeightAndCount should have the proper Weight and NormalizedWeight
    newWc.weight shouldBe Weight(0.59)
    wts.weight shouldBe NormalizedWeight(Weight(0.59))
  }

  it should "include, update, and convert correctly (when NOT full)" in {
    val newWc = new WeightAndCount(MaxValue, 0)
    val groupCubeSize = 15
    val baseWeight = 0.54

    // Weights from Weight(0.55) to Weight(0.64)
    (1 to 10).foreach { i =>
      val w = Weight(baseWeight + (i.toDouble / 100))
      if (newWc.shouldInclude(w, groupCubeSize)) newWc.update(w)
    }
    val wts = newWc.toWeightAndTreeSize(groupCubeSize)

    // With the groupCubeSize being 15, it should accept all 10 records
    newWc.count shouldBe 10
    newWc.cubeSize shouldBe 10
    wts.treeSize shouldBe 10

    // A full new WeightAndCount should have the proper Weight and NormalizedWeight
    newWc.weight shouldBe Weight(0.64)
    wts.weight shouldBe NormalizedWeight(groupCubeSize, 10)

  }

  "An InnerCubeWeightAndCount" should "be limited by existingWeight" in {
    val existingWeight = Weight(0.6)
    val innerWc = new InnerCubeWeightAndCount(existingWeight)
    val groupCubeSize = 15
    val baseWeight = 0.54

    // Weights from Weight(0.55) to Weight(0.64)
    (1 to 10).foreach { i =>
      val w = Weight(baseWeight + (i.toDouble / 100))
      if (innerWc.shouldInclude(w, groupCubeSize)) innerWc.update(w)
    }

    val wts = innerWc.toWeightAndTreeSize(groupCubeSize)
    // It should accept only the first 5 elements that have w < 0.6
    innerWc.count shouldBe 5
    innerWc.cubeSize shouldBe 5
    wts.treeSize shouldBe 5

    // It weight should be reduced to Weight(0.5)
    innerWc.weight shouldBe Weight(0.59)
    wts.weight shouldBe 0.59 +- 1e-8
  }

  "LeafCubeWeightAndCount" should "include, update, and convert correctly (when full)" in {
    val leafWc = new LeafCubeWeightAndCount(5)
    val groupCubeSize = 10
    val baseWeight = 0.54

    // Weights from Weight(0.55) to Weight(0.64)
    (1 to 10).foreach { i =>
      val w = Weight(baseWeight + (i.toDouble / 100))
      if (leafWc.shouldInclude(w, groupCubeSize)) leafWc.update(w)
    }
    val wts = leafWc.toWeightAndTreeSize(groupCubeSize)

    // With the groupCubeSize being 10, it should only accept the first
    // groupCubeSize - start = 5 records
    leafWc.count shouldBe 10
    leafWc.cubeSize shouldBe 5
    wts.treeSize shouldBe 5

    // The fifth and the last record it accepted has w = Weight(0.59)
    leafWc.weight shouldBe Weight(0.59)
    // A full LeafCubeWeightAndCount should have the proper NormalizedWeight
    wts.weight shouldBe NormalizedWeight(Weight(0.59))
  }

  it should "include, update, and convert correctly (when NOT full)" in {
    val leafWc = new LeafCubeWeightAndCount(5)
    val groupCubeSize = 15
    val baseWeight = 0.54

    // Weights from Weight(0.55) to Weight(0.64)
    (1 to 5).foreach { i =>
      val w = Weight(baseWeight + (i.toDouble / 100))
      if (leafWc.shouldInclude(w, groupCubeSize)) leafWc.update(w)
    }
    val wts = leafWc.toWeightAndTreeSize(groupCubeSize)

    // With the groupCubeSize being 15, it has the space for 10 records so
    // it should accept all 5
    leafWc.count shouldBe 10
    leafWc.cubeSize shouldBe 5
    wts.treeSize shouldBe 5

    // A full LeafCubeWeightAndCount should have the proper NormalizedWeight
    wts.weight shouldBe NormalizedWeight(groupCubeSize, 5)
  }

}
