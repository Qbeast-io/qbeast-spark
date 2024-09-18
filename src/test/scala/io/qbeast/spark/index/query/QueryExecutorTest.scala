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
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.core.transform.EmptyTransformer
import io.qbeast.spark.delta.QbeastFiles
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap

class QueryExecutorTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  behavior of "QueryExecutor"

  it should "find all sample files" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = getQbeastSnapshot(tmpdir)

    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute(new Path(tmpdir)).map(_.getPath.getName).toSet

    val diff = allFiles.toSet -- matchFiles

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles shouldBe allFiles.toSet

  })

  it should "filter the files according to the weight" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = getQbeastSnapshot(tmpdir)

    val filters = Seq(weightFilters(WeightRange(Weight.MinValue, Weight(0.001))))
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute(new Path(tmpdir)).map(_.getPath.getName).toSet

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "filter the files according to the space" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = getQbeastSnapshot(tmpdir)

    val filters = Seq(expr("a >= 2 and a < 10").expr)
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute(new Path(tmpdir)).map(_.getPath.getName).toSet

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "find all files in different revisions" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val differentRevision =
      source
        .withColumn("a", col("a") * 5)
        .withColumn("c", col("c") * 6)

    writeTestData(differentRevision, Seq("a", "c"), 10000, tmpdir, "append")

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = getQbeastSnapshot(tmpdir)

    // Including the staging revision
    qbeastSnapshot.loadAllRevisions.size shouldBe 3

    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute(new Path(tmpdir)).map(_.getPath.getName).toSet

    val diff = allFiles.toSet -- matchFiles
    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles shouldBe allFiles.toSet

  })

  it should "skip blocks with maxWeight < weightRange.from" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = createDF(500000, spark).toDF()

      writeTestData(df, Seq("a", "c"), 10000, tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = getQbeastSnapshot(tmpDir)

      val weightRange = WeightRange(Weight(3), Weight(5))
      val expressionFilters = weightFilters(weightRange)
      val querySpecBuilder = new QuerySpecBuilder(Seq(expressionFilters))

      val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)

      val allBlocks =
        deltaLog.update().allFiles.collect().map(QbeastFiles.fromAddFile(2)).flatMap(_.blocks)

      val matchingBlocks = queryExecutor.execute(new Path(tmpDir)).map(_.getPath.getName).toSet

      matchingBlocks shouldBe allBlocks
        .filter(a => a.maxWeight >= weightRange.from && a.minWeight < weightRange.to)
        .map(_.filePath)
        .toSet
    })

  it should "handle index with a missing inner cube" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)

    val qbeastSnapshot = getQbeastSnapshot(tmpdir)
    val revision = qbeastSnapshot.loadLatestRevision
    val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)

    val innerCubesLevel1 =
      indexStatus.cubesStatuses.keys.filter(cube =>
        cube.depth == 1 && indexStatus.cubesStatuses.contains(cube))
    val cubeToRemove = if (innerCubesLevel1.isEmpty) CubeId.root(2) else innerCubesLevel1.head

    val faultyIndexStatus =
      indexStatus.copy(cubesStatuses = indexStatus.cubesStatuses - cubeToRemove)

    val querySpecBuilder = new QuerySpecBuilder(Seq.empty)
    val querySpec = querySpecBuilder.build(revision).head
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    import spark.implicits._
    val matchCubes = queryExecutor
      .executeRevision(querySpec, faultyIndexStatus)
      .toList
      .toDS()
      .select(struct("*").as("cubeId"))
    val indexFiles: Dataset[DenormalizedBlock] =
      QbeastTable.forPath(spark, tmpdir).getDenormalizedBlocks(Some(revision.revisionID))
    val matchFiles = indexFiles
      .join(matchCubes, "cubeId")
      .select("filePath")
      .as[String]
      .collect()
    val allFiles = deltaLog.update().allFiles.collect().map(_.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 1
  })

  it should "find the max value when filtering" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(50000, spark).toDF().coalesce(4)

    writeTestData(source, Seq("a", "c"), 4000, tmpdir)

    val indexed = spark.read.format("qbeast").load(tmpdir)

    indexed.where("a == 49999").count shouldBe 1
    indexed.where("c == 49999.0").count shouldBe 1
  })

  it should "filter correctly using GreaterThan and LessThanOrEqual" in withSparkAndTmpDir(
    (spark, tmpdir) => {
      val source = createDF(50000, spark).toDF().coalesce(4)

      writeTestData(source, Seq("a", "c"), 4000, tmpdir)

      val indexed = spark.read.format("qbeast").load(tmpdir)

      indexed.where("a > 1").count shouldBe 49998
      indexed.where("a > 49998").count shouldBe 1

      indexed.where("a <= 1").count shouldBe 2
      indexed.where("a <= 49999").count shouldBe 50000

    })

  it should "filter cube blocks by query weight range" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val samplingFraction = 0.5
      val root = CubeId.root(2)
      val c1 = root.firstChild

      val rootBlock =
        s"""{
          "cubeId": "${root.string}",
          "minWeight": ${Weight(0.0).value},
          "maxWeight": ${Weight(0.1).value},
          "elementCount": 1, "replicated": false}""".stripMargin
      val c1Block1 =
        s"""{
          "cubeId": "${c1.string}",
          "minWeight": ${Weight(0.1).value},
          "maxWeight": ${Weight(0.2).value},
          "elementCount": 1,"replicated": false}""".stripMargin
      val c1Block2 =
        s"""{"cubeId": "${c1.string}",
          "minWeight": ${Weight(0.7).value},
          "maxWeight": ${Weight(0.8).value},
          "elementCount": 1, "replicated": false}"""

      val Seq(rb1, c1b1, c1b2) = QbeastFiles
        .fromAddFile(2)(
          AddFile(
            path = tmpDir,
            partitionValues = Map.empty,
            size = 1L,
            modificationTime = 1L,
            dataChange = true,
            tags = Map("blocks" -> s"""[$rootBlock, $c1Block1, $c1Block2]""")))
        .blocks

      val indexStatus = IndexStatus(
        Revision.firstRevision(
          new QTableID(tmpDir),
          1,
          Vector(EmptyTransformer("t1"), EmptyTransformer("t2"))),
        cubesStatuses = SortedMap(
          root -> CubeStatus(root, Weight(0.1), 0.1, rb1 :: Nil),
          c1 -> CubeStatus(root, Weight(0.2), 0.2, c1b1 :: c1b2 :: Nil)))

      val qbeastSnapshot = getQbeastSnapshot(tmpDir)
      val querySpecBuilder = new QuerySpecBuilder(Seq.empty[Expression])

      val executor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)

      val executeRevision: PrivateMethod[Seq[Block]] = PrivateMethod[Seq[Block]]('executeRevision)
      val outputBlocks = executor invokePrivate executeRevision(
        QuerySpec(WeightRange(Weight(0d), Weight(samplingFraction)), AllSpace()),
        indexStatus)

      // c1Block2 should not be part of outputBlocks
      outputBlocks.size shouldBe 2
      outputBlocks.contains(c1b2) shouldBe false
    })

}
