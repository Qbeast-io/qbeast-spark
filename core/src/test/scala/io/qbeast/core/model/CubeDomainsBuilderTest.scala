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

import io.qbeast.core.index.NormalizedWeight
import io.qbeast.core.transform.HashTransformer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.PrivateMethodTester

import scala.util.Random

class CubeDomainsBuilderTest extends AnyFlatSpec with Matchers with PrivateMethodTester {

  private val point = Point(0.9, 0.1)
  private val dcs = 400

  private val transformers =
    Vector(HashTransformer("a", StringDataType), HashTransformer("b", StringDataType))

  private val transformations = transformers.map(t => t.makeTransformation(r => r))

  private val rev =
    Revision
      .firstRevision(QTableID("test"), dcs, transformers, transformations)
      .copy(revisionID = 1)

  private def createCubeDomainsBuilder(
      existingCubeWeights: Map[CubeId, Weight] = Map.empty,
      replicatedOrAnnouncedSet: Set[CubeId] = Set.empty,
      desiredCubeSize: Int = rev.desiredCubeSize,
      numPartitions: Int = 10,
      elementCount: Long = 100L,
      bufferCapacity: Long = 100000L): CubeDomainsBuilder = {
    // The default setting creates a groupCubeSize of 40
    CubeDomainsBuilder(
      existingCubeWeights,
      replicatedOrAnnouncedSet,
      desiredCubeSize,
      numPartitions,
      elementCount,
      bufferCapacity)
  }

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
    val builder = createCubeDomainsBuilder()
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    // Cube -> (NormalizedWeight, cubeSize)
    val wts = builder invokePrivate computeWeightsAndCubeSizes()

    // Inner cubes are full with gcs elements each, the remaining is stored in the leaf cube
    wts.mapValues(_.treeSize).toSeq.sortBy(_._1).map(_._2) shouldBe Seq(40d, 40d, 20d)
  }

  it should "assign the correct NormalizedWeight to a leaf cube" in {
    val builder = createCubeDomainsBuilder()
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    // Cube -> (NormalizedWeight, cubeSize)
    val wts = builder invokePrivate computeWeightsAndCubeSizes()
    // The leaf cube has 20 elements, which is < gcs
    wts.toSeq.maxBy(_._1)._2.weight shouldBe NormalizedWeight(40, 20L)
  }

  it should "give the values whether the data is sorted or not" in {
    val randomBuilder = createCubeDomainsBuilder()
    val sortedBuilder = createCubeDomainsBuilder()

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
    val builder = createCubeDomainsBuilder()
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
    val builder = createCubeDomainsBuilder(elementCount = 1000, bufferCapacity = 800)
    (1 to 1000).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    val result = builder.result()
    // A new index in created each time the bufferCapacity is exceeded
    result.count { cd =>
      val cubeId = rev.createCubeId(cd.cubeBytes)
      val isRoot = cubeId.depth == 0
      isRoot
    } shouldBe 2
  }

  it should "handle empty partitions" in {
    val builder = createCubeDomainsBuilder()
    val domains = builder.result()

    domains.isEmpty shouldBe true
  }

  it should "add maxWeight to the child of a replicated or announced cube" in {
    val root = rev.createCubeIdRoot()
    val builder = createCubeDomainsBuilder(replicatedOrAnnouncedSet = Set(root))
    builder.update(point, Weight(2))

    val Seq(c0, c1) = builder.result().map(cd => rev.createCubeId(cd.cubeBytes)).sorted
    c0 shouldBe root
    c1.parent shouldBe Some(c0)
  }

  it should "calculate domain for the root" in {
    val builder = createCubeDomainsBuilder()
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    val partitionCubeDomains =
      builder.result().map(cd => (rev.createCubeId(cd.cubeBytes), cd.domain)).toMap

    partitionCubeDomains(rev.createCubeIdRoot()) shouldBe 100d
  }

  it should "not duplicate elements in result" in {
    val builder = createCubeDomainsBuilder()
    (1 to 100).foreach(_ => builder.update(point, Weight(Random.nextInt())))

    val result = builder.result()
    result.size shouldBe result.distinct.size
  }

}
