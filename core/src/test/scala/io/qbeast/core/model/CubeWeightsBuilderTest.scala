package io.qbeast.core.model

import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class CubeWeightsBuilderTest extends AnyFlatSpec with Matchers with PrivateMethodTester {

  private val point = Point(0.9, 0.1)
  private val dcs = 400

  private val rev =
    Revision.emptyRevision(QTableID("test"), dcs, Seq("a", "b")).copy(revisionID = 1)

  private val emptyIndexStatus = IndexStatus.empty(rev)

  private val computeWeightsAndCubeSizes: PrivateMethod[Map[CubeId, WeightAndTreeSize]] =
    PrivateMethod[Map[CubeId, WeightAndTreeSize]]('computeWeightsAndSizes)

  private val computeCubeDomains: PrivateMethod[Seq[CubeDomain]] =
    PrivateMethod[Seq[CubeDomain]]('computeCubeDomains)

  private def checkDecreasingBranchDomain(
      cube: CubeId,
      domainMap: Map[CubeId, Double],
      parentDomain: Double): Boolean = {
    val cubeDomain = domainMap(cube)
    cubeDomain < parentDomain && cube.children
      .filter(domainMap.contains)
      .forall(c => checkDecreasingBranchDomain(c, domainMap, cubeDomain))
  }

  "CubeWeightsBuilder" should "add weights to the cube until it is full" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 1000, 100000)
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    // Cube -> (NormalizedWeight, cubeSize)
    val wts = builder invokePrivate computeWeightsAndCubeSizes()

    // Inner cubes are full with gcs elements each, the remaining is stored in the leaf cube
    // groupCubeSize = 40
    wts.mapValues(_.treeSize).toSeq.sortBy(_._1).map(_._2) shouldBe Seq(40d, 40d, 20d)
  }

  it should "assign the correct NormalizedWeight to a leaf cube" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 1000, 100000)
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    // Cube -> (NormalizedWeight, cubeSize)
    val wts = builder invokePrivate computeWeightsAndCubeSizes()
    // The leaf cube has 20 elements, which is < gcs
    wts.toSeq.maxBy(_._1)._2.weight shouldBe NormalizedWeight(dcs, 20)
  }

  it should "give the values whether the data is sorted or not" in {
    val randomBuilder = new CubeWeightsBuilder(emptyIndexStatus, 10, 1000, 100000)
    val sortedBuilder = new CubeWeightsBuilder(emptyIndexStatus, 10, 1000, 100000)

    val weights = (1 to 100).map(_ => Weight(Random.nextInt()))
    weights.foreach(w => randomBuilder.update(point, w))
    weights.sorted.foreach(w => sortedBuilder.update(point, w))

    val randomWeightsAndSizes =
      randomBuilder.result().map(cd => (rev.createCubeId(cd.cubeBytes), cd.domain))
    val sortedWeightsAndSizes =
      sortedBuilder.result().map(cd => (rev.createCubeId(cd.cubeBytes), cd.domain))

    randomWeightsAndSizes shouldBe sortedWeightsAndSizes
  }

  it should "compute domains correctly" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 10000, 100000)
    (1 to 100).foreach(_ =>
      builder.update(Point(Random.nextFloat(), Random.nextFloat()), Weight(Random.nextInt())))

    val wts = builder invokePrivate computeWeightsAndCubeSizes()
    val domains = builder invokePrivate computeCubeDomains(wts)
    // Should have the same number of cubes
    wts.size shouldBe domains.size

    val domainMap = domains.map(cd => rev.createCubeId(cd.cubeBytes) -> cd.domain).toMap
    checkDecreasingBranchDomain(rev.createCubeIdRoot(), domainMap, 101d) shouldBe true
  }

  it should "respect bufferCapacity" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 10000, 800)
    (1 to 1000).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    val result = builder.result()
    // A new index in created each time the bufferCapacity is exceeded
    result.count(cd => rev.createCubeId(cd.cubeBytes).depth == 0) shouldBe 2
  }

  it should "handle empty partitions" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 1000, 100000)
    val domains = builder.resultInternal()

    domains.isEmpty shouldBe true
  }

  it should "add maxWeight to the child of an announced cube" in {
    val root = rev.createCubeIdRoot()
    val builder =
      new CubeWeightsBuilder(emptyIndexStatus.copy(announcedSet = Set(root)), 10, 1000, 100000)
    builder.update(point, Weight(2))

    val Seq(c0, c1) = builder.result().map(cd => rev.createCubeId(cd.cubeBytes)).sorted
    c0 shouldBe root
    c1.parent shouldBe Some(c0)
  }

  it should "add maxWeight to the child of a replicated cube" in {
    val root = rev.createCubeIdRoot()
    val builder =
      new CubeWeightsBuilder(emptyIndexStatus.copy(replicatedSet = Set(root)), 10, 1000, 100000)
    builder.update(point, Weight(2))

    val Seq(c0, c1) = builder.result().map(cd => rev.createCubeId(cd.cubeBytes)).sorted
    c0 shouldBe root
    c1.parent shouldBe Some(c0)
  }

  it should "calculate domain for the root" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 1000, 1000000)
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    val partitionCubeDomains =
      builder.result().map(cd => (rev.createCubeId(cd.cubeBytes), cd.domain)).toMap

    partitionCubeDomains(rev.createCubeIdRoot()) shouldBe 100d
  }

  it should "not duplicate elements in result" in {
    val builder = new CubeWeightsBuilder(emptyIndexStatus, 10, 10000, 100000)
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    val result = builder.result()
    result.size shouldBe result.distinct.size
  }
}
