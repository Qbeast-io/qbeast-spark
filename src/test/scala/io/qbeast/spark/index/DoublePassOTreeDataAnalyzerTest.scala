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
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer._
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

class DoublePassOTreeDataAnalyzerTest
    extends QbeastIntegrationTestSpec
    with SparkRevisionChangesUtils {

  private def createDF(size: Int, spark: SparkSession): Dataset[T3] = {
    import spark.implicits._
    spark.range(size).map(i => T3(i, i.toDouble, i.toString, i.toFloat))
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

  "skipCube" should "not compute weight for descendents of leaf cubes" in {
    val root = CubeId.root(2)
    val c1 = root.firstChild
    val c2 = c1.firstChild
    val c3 = c2.firstChild

    skipCube(root, Map.empty) shouldBe false

    skipCube(c1, Map(root -> 0.9)) shouldBe false

    skipCube(c2, Map(root -> 0.9, c1 -> 1d)) shouldBe true

    skipCube(c3, Map(root -> 0.9, c1 -> 1d)) shouldBe true
  }

  "computePartitionCubeDomains" should "compute root for all partitions" in withSpark { spark =>
    import spark.implicits._
    // Repartition the data to make sure no empty partitions
    // The amount of data we have for each partition is much smaller
    // than the bufferCapacity
    val data = createDF(100000, spark).repartition(50)
    val columnsToIndex = data.columns.mkString(",")
    val options = QbeastOptions(Map("columnsToIndex" -> columnsToIndex, "cubeSize" -> "1000"))
    val startingRevision =
      SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
    val indexStatus = IndexStatus.empty(startingRevision)
    val (revisionChanges, numElements) =
      computeRevisionChanges(indexStatus.revision, options, data.toDF())
    val (isNewRevision, revisionToUse) = revisionChanges match {
      case None => (false, indexStatus.revision)
      case Some(revisionChange) => (true, revisionChange.createNewRevision)
    }

    val weightedDataFrame = data.withColumn(weightColumnName, qbeastHash(rand()))

    // Compute partition-level cube domains
    val inputDataPartitionCubeDomains: Dataset[CubeDomain] =
      weightedDataFrame
        .transform(
          computePartitionCubeDomains(
            numElements,
            revisionToUse,
            indexStatus,
            isNewRevision = isNewRevision))

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

  "computeInputDataCubeDomains" should "compute input cube domains correctly" in withSpark {
    spark =>
      val data = createDF(10000, spark)
      val columnsToIndex = data.columns.mkString(",")
      val options = QbeastOptions(Map("columnsToIndex" -> columnsToIndex, "cubeSize" -> "1000"))
      val startingRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
      val indexStatus = IndexStatus.empty(startingRevision)
      val (revisionChanges, numElements) =
        computeRevisionChanges(indexStatus.revision, options, data.toDF())
      val (isNewRevision, revisionToUse) = revisionChanges match {
        case None => (false, indexStatus.revision)
        case Some(revisionChange) => (true, revisionChange.createNewRevision)
      }

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
              isNewRevision = isNewRevision))
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

  "estimateUpdatedCubeWeights" should "estimate NormalizedWeights correctly" in withSpark {
    spark =>
      val data = createDF(10000, spark)
      val columnsToIndex = data.columns.mkString(",")
      val options = QbeastOptions(Map("columnsToIndex" -> columnsToIndex, "cubeSize" -> "1000"))
      val startingRevision =
        SparkRevisionFactory.createNewRevision(QTableID("test"), data.schema, options)
      val indexStatus = IndexStatus.empty(startingRevision)
      val (revisionChanges, numElements) =
        computeRevisionChanges(indexStatus.revision, options, data.toDF())
      val (isNewRevision, revisionToUse) = revisionChanges match {
        case None => (false, indexStatus.revision)
        case Some(revisionChange) => (true, revisionChange.createNewRevision)
      }

      val weightedDataFrame = data.withColumn(weightColumnName, qbeastHash(rand()))

      // Compute cube domains for the input data
      val inputDataCubeDomains: Map[CubeId, Double] =
        weightedDataFrame
          .transform(
            computeInputDataCubeDomains(
              numElements,
              revisionToUse,
              indexStatus,
              isNewRevision = isNewRevision))
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

  "AnalyzeDataframeDeterminism method" should "detect when the column to index is non deterministic" in withSpark {
    spark =>
      val nonDeterministicUDF = udf(() => Math.random()).asNondeterministic()

      val df = spark
        .range(1000)
        .toDF("id")
        .withColumn("non_deterministic_col", nonDeterministicUDF())
      val revision = Revision(
        revisionID = 1L,
        timestamp = System.currentTimeMillis(),
        tableID = QTableID("id"),
        desiredCubeSize = 5000,
        columnTransformers = Seq(LinearTransformer("id", IntegerDataType)).toIndexedSeq,
        transformations = Seq.empty.toIndexedSeq)
      val isDeterministic = DoublePassOTreeDataAnalyzer.analyzeDataFrameDeterminism(df, revision)
      isDeterministic shouldBe false
  }

  it should "return true when the column to index is not bounded" in withSpark { spark =>
    val nonDeterministicUDF = udf(() => Math.random()).asNondeterministic()

    val df = spark
      .range(1000)
      .toDF("id")
      .withColumn("non_deterministic_col", nonDeterministicUDF())
    val revision = Revision(
      revisionID = 1L,
      timestamp = System.currentTimeMillis(),
      tableID = QTableID("id"),
      desiredCubeSize = 5000,
      columnTransformers = Seq(HashTransformer("id", IntegerDataType)).toIndexedSeq,
      transformations = Seq.empty.toIndexedSeq)
    val isDeterministic = DoublePassOTreeDataAnalyzer.analyzeDataFrameDeterminism(df, revision)
    isDeterministic shouldBe true
  }

}
