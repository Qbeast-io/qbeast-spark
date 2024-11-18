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
package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer._
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import io.qbeast.spark.utils.SparkToQTypesUtils
import io.qbeast.IISeq
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper

class DoublePassOTreeDataAnalyzerTest extends QbeastIntegrationTestSpec {

  private def createDF(size: Int, spark: SparkSession): Dataset[T3] = {
    import spark.implicits._

    spark
      .range(size)
      .map(i => T3(i, i.toDouble, i.toString, i.toFloat))

  }

  private def createTransformers(columnsSchema: Seq[StructField]): IISeq[Transformer] = {
    columnsSchema
      .map(field => Transformer(field.name, SparkToQTypesUtils.convertDataTypes(field.dataType)))
      .toIndexedSeq
  }

  private def checkDecreasingBranchDomain(
      cube: CubeId,
      domainMap: Map[CubeId, Double],
      parentDomain: Double): Boolean = {
    val cubeDomain = domainMap(cube)
    cubeDomain < parentDomain && cube.children
      .filter(domainMap.contains)
      .forall(c => checkDecreasingBranchDomain(c, domainMap, cubeDomain))
  }

  it should "calculateRevisionChanges correctly on single column" in withSpark { spark =>
    val dataFrame = createDF(10000, spark).toDF()
    val columnsToIndex = Seq("c")
    val columnsSchema = dataFrame.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)
    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    revision.revisionID shouldBe 1L
    revision.columnTransformers.size shouldBe 1
    revision.transformations.size shouldBe 1

    val columnTransformer = revision.columnTransformers.head
    columnTransformer.columnName shouldBe "c"
    columnTransformer.spec shouldBe "c:hashing"

    val transformation = revision.transformations.head
    transformation mustBe a[HashTransformation]

  }

  it should "calculateRevisionChanges correctly on hashing types" in withSpark { spark =>
    import spark.implicits._

    val dataFrame = spark
      .range(10000)
      .map(i => TestStrings(s"$i", s"${i * i}", s"${i * 2}"))

    val columnsToIndex = Seq("a", "b", "c")
    val columnsSchema = dataFrame.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)
    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(dataFrame.toDF(), columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    revision.columnTransformers.length shouldBe columnsToIndex.length
    revision.transformations.length shouldBe columnsToIndex.length
    revision.transformations.foreach(t => t shouldBe a[HashTransformation])
  }

  it should "calculateRevisionChanges correctly on different types" in withSpark { spark =>
    val dataFrame = createDF(10001, spark).toDF()
    val columnsToIndex = dataFrame.columns
    val columnsSchema = dataFrame.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)
    val revisionChanges =
      calculateRevisionChanges(dataFrameStats, emptyRevision)
    revisionChanges shouldBe defined
    revisionChanges.get.supersededRevision shouldBe emptyRevision
    revisionChanges.get.transformationsChanges.count(_.isDefined) shouldBe columnsToIndex.length
    revisionChanges.get.desiredCubeSizeChange shouldBe None
    revisionChanges.get.columnTransformersChanges shouldBe Vector.empty

    val revision = revisionChanges.get.createNewRevision

    revision.revisionID shouldBe 1L
    revision.columnTransformers.size shouldBe columnsToIndex.length
    revision.columnTransformers.map(_.columnName) shouldBe columnsToIndex
    revision.transformations.size shouldBe columnsToIndex.length

    val zero = revision.transformations.head
    val one = revision.transformations(1)
    val two = revision.transformations(2)
    val three = revision.transformations(3)

    zero should matchPattern { case LinearTransformation(0, 10000, _, LongDataType) => }
    one should matchPattern { case LinearTransformation(0.0, 10000.0, _, DoubleDataType) => }
    two should matchPattern { case HashTransformation(_) => }
    three should matchPattern { case LinearTransformation(0.0f, 10000.0f, _, FloatDataType) => }

  }

  it should "calculateRevisionChanges when the space gets bigger" in withSpark { spark =>
    import spark.implicits._

    val data = createDF(10001, spark)
    val columnsToIndex = data.columns
    val columnsSchema = data.schema.filter(f => columnsToIndex.contains(f.name))
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)

    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val revision =
      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision

    val spaceMultiplier = 2
    val newRevisionDataFrame =
      data.map(t3 =>
        t3.copy(
          a = t3.a * spaceMultiplier,
          b = t3.b * spaceMultiplier,
          d = t3.d * spaceMultiplier))

    val newDataFrameStats = getDataFrameStats(newRevisionDataFrame.toDF(), columnTransformers)
    val revisionChanges =
      calculateRevisionChanges(newDataFrameStats, revision)
    val newRevision = revisionChanges.get.createNewRevision

    newRevision.revisionID shouldBe 2L
    newRevision.columnTransformers.size shouldBe columnsToIndex.length
    newRevision.columnTransformers.map(_.columnName) shouldBe columnsToIndex
    newRevision.transformations.size shouldBe columnsToIndex.length

    val zero = newRevision.transformations.head
    val one = newRevision.transformations(1)
    val two = newRevision.transformations(2)
    val three = newRevision.transformations(3)

    zero should matchPattern { case LinearTransformation(0, 20000, _, LongDataType) => }
    one should matchPattern { case LinearTransformation(0.0, 20000.0, _, DoubleDataType) => }
    two should matchPattern { case HashTransformation(_) => }
    three should matchPattern { case LinearTransformation(0.0f, 20000.0f, _, FloatDataType) =>
    }

  }

  it should "not compute weight for descendents of leaf cubes" in {
    val root = CubeId.root(2)
    val c1 = root.firstChild
    val c2 = c1.firstChild
    val c3 = c2.firstChild

    skipCube(root, Map.empty) shouldBe false

    skipCube(c1, Map(root -> 0.9)) shouldBe false

    skipCube(c2, Map(root -> 0.9, c1 -> 1d)) shouldBe true

    skipCube(c3, Map(root -> 0.9, c1 -> 1d)) shouldBe true
  }

  it should "compute root for all partitions" in withSpark { spark =>
    import spark.implicits._
    // Repartition the data to make sure no empty partitions
    // The amount of data we have for each partition is much smaller
    // than the bufferCapacity
    val data = createDF(100000, spark).repartition(50)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val indexStatus = IndexStatus.empty(
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq))

    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val numElements = dataFrameStats.getAs[Long]("count")

    val revisionToUse =
      calculateRevisionChanges(dataFrameStats, indexStatus.revision).get.createNewRevision

    val weightedDataFrame = data.withColumn(weightColumnName, qbeastHash(rand()))

    // Compute partition-level cube domains
    val inputDataPartitionCubeDomains: Dataset[CubeDomain] =
      weightedDataFrame
        .transform(
          computePartitionCubeDomains(
            numElements,
            revisionToUse,
            indexStatus,
            isNewRevision = true))

    val cubeCount = inputDataPartitionCubeDomains
      .groupBy("cubeBytes")
      .count()
      .map { row =>
        val bytes = row.getAs[Array[Byte]](0)
        val cnt = row.getAs[Long](1)
        (revisionToUse.createCubeId(bytes), cnt)
      }
      .collect()
      .toMap

    val root = revisionToUse.createCubeIdRoot()
    cubeCount(root).toInt shouldBe 50
  }

  it should "compute delta cube domains correctly" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val emptyRevision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val indexStatus = IndexStatus.empty(emptyRevision)

    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val numElements = dataFrameStats.getAs[Long]("count")

    // The revision to use
    val revisionToUse =
      calculateRevisionChanges(dataFrameStats, indexStatus.revision).get.createNewRevision

    // Add a random weight column
    val weightedDataFrame = data.withColumn(weightColumnName, qbeastHash(rand()))

    // Compute cube domains for the input data
    val inputDataCubeDomains: Map[CubeId, Double] =
      weightedDataFrame
        .transform(
          computeInputDataCubeDomains(
            numElements,
            revisionToUse,
            indexStatus,
            isNewRevision = true))
        .collect()
        .toMap

    // Cube domains should monotonically decrease
    checkDecreasingBranchDomain(
      revisionToUse.createCubeIdRoot(),
      inputDataCubeDomains,
      10002d) shouldBe true

    // Root should be present in all partitions, its global domain should be the elementCount
    inputDataCubeDomains(revisionToUse.createCubeIdRoot()).toLong shouldBe numElements
  }

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
    val cubeStatuses = blocks
      .map(b => b.cubeId -> CubeStatus(b.cubeId, b.maxWeight, b.maxWeight.fraction, b :: Nil))
      .toMap

    val domains = computeExistingCubeDomains(cubeStatuses)
    domains(root) shouldBe 30d
    domains(c1) shouldBe domains(root) * (12d / 20)
    domains(c2) shouldBe domains(root) * (8d / 20)
    domains(c3) shouldBe domains(c1)
  }

  it should "estimate NormalizedWeights correctly" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val indexStatus = IndexStatus.empty(
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq))

    val dataFrameStats = getDataFrameStats(data.toDF(), columnTransformers)
    val numElements = dataFrameStats.getAs[Long]("count")

    val revisionToUse =
      calculateRevisionChanges(dataFrameStats, indexStatus.revision).get.createNewRevision

    val weightedDataFrame = data.withColumn(weightColumnName, qbeastHash(rand()))

    // Compute cube domains for the input data
    val inputDataCubeDomains: Map[CubeId, Double] =
      weightedDataFrame
        .transform(
          computeInputDataCubeDomains(
            numElements,
            revisionToUse,
            indexStatus,
            isNewRevision = true))
        .collect()
        .toMap

    // Merge globalCubeDomain with the existing cube domains
    val updatedCubeDomains: Map[CubeId, Double] =
      computeUpdatedCubeDomains(inputDataCubeDomains, indexStatus, isNewRevision = true)

    // Populate NormalizedWeight level-wise from top to bottom
    val updatedCubeWeights: Map[CubeId, Weight] =
      estimateUpdatedCubeWeights(
        updatedCubeDomains.toSeq,
        indexStatus,
        revisionToUse,
        isNewRevision = true)

    // Cubes with a weight lager than 1d should not have children
    val leafCubesByWeight = updatedCubeWeights.filter(cw => cw._2.fraction >= 1d).keys
    leafCubesByWeight.exists(cube =>
      cube.children.exists(updatedCubeWeights.contains)) shouldBe false

    // Test estimatedCubeWeights is monotonically increasing in all branches
    updatedCubeWeights.toSeq
      .sortBy(_._1)
      .foreach { case (cube, weight) =>
        cube.children
          .filter(updatedCubeWeights.contains)
          .foreach(child => weight < updatedCubeWeights(child))
      }
  }

  "computeInputDataBlockElementCounts" should "should calculate correct block element counts" in {
    // Sample data
    val root = CubeId.root(2)
    val kids = root.children // 1000 => Domain  2655 -> ebs(deltaW-> 0.1 * 100) =>10
    val a = kids.next() // 520 => Domain 520 -> ebs(deltaW-> 0.1 * 52) => 5
    val b = kids.next() // 35 => Domain 35 -> ebs(deltaW-> 0.2 * 35) => 6
    val c = kids.next() // 1000 => Domain 2100 -> ebs(deltaW-> 0.3 * 100) => 29
    val cKids = c.children
    val ca = cKids.next() // 500 -> ebs(deltaW -> 0.1 * 50) => 5
    val cb = cKids.next() // 600 -> ebs(deltaW -> 0.2* 60) => 11

    val inputDataCubeDomains: Map[CubeId, Double] =
      Map(root -> 100d, a -> 52d, b -> 35d, c -> 100d, ca -> 50d, cb -> 60d)

    val updatedCubeWeights: Map[CubeId, Weight] =
      Map(
        root -> Weight(0.1),
        a -> Weight(0.2),
        b -> Weight(0.3),
        c -> Weight(0.4),
        ca -> Weight(0.5),
        cb -> Weight(0.6))

    // Expected results
    val expectedBlockSizes: Map[CubeId, Long] =
      Map(root -> 10L, a -> 5L, b -> 6L, c -> 29L, ca -> 5L, cb -> 11L)

    // Call the method
    val result =
      DoublePassOTreeDataAnalyzer.computeInputDataBlockElementCounts(
        inputDataCubeDomains,
        updatedCubeWeights)

    // Assert the results
    result shouldBe expectedBlockSizes

  }

}
