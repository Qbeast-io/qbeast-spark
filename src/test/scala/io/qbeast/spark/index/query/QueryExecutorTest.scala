package io.qbeast.spark.index.query

import io.qbeast.core.model.{CubeId, Weight, WeightRange}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.delta.DeltaQbeastSnapshot

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, expr}

class QueryExecutorTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  behavior of "QueryExecutor"

  it should "find all sample files" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path).toSet

    val diff = allFiles.toSet -- matchFiles.toSet

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "filter the files according to the weight" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

    val filters = Seq(weightFilters(WeightRange(Weight.MinValue, Weight(0.001))))
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path).toSet

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "filter the files according to the space" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

    val filters = Seq(expr("a >= 2 and a < 10").expr)
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path).toSet

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
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

    // Including the staging revision
    qbeastSnapshot.loadAllRevisions.size shouldBe 3

    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "skip blocks with maxWeight < weightRange.from" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = createDF(500000, spark).toDF()

      writeTestData(df, Seq("a", "c"), 10000, tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

      val weightRange = WeightRange(Weight(3), Weight(5))
      val expressionFilters = weightFilters(weightRange)
      val querySpecBuilder = new QuerySpecBuilder(Seq(expressionFilters))

      val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)

      val allBlocks =
        deltaLog.update().allFiles.collect().map(IndexFiles.fromAddFile(2)).flatMap(_.blocks)

      val matchingBlocks = queryExecutor.execute()

      allBlocks.filter(_.maxWeight < weightRange.from).foreach { block =>
        matchingBlocks should not contain (block)
      }
    })

  it should "handle index with a missing inner cube" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)

    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
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
    val matchFiles = queryExecutor
      .executeRevision(querySpec, faultyIndexStatus)
      .map(_.file.path)

    val allFiles = deltaLog.update().allFiles.collect().map(_.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 1
  })

  it should "find the max value when filtering" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(50000, spark).toDF().coalesce(4)

    writeTestData(source, Seq("a", "c"), 4000, tmpdir)

    val indexed = spark.read.format("qbeast").load(tmpdir)

    indexed.where("a == 50000").count shouldBe 1
    indexed.where("c == 50000.0").count shouldBe 1
  })

  it should "filter correctly using GreaterThan and LessThanOrEqual" in withSparkAndTmpDir(
    (spark, tmpdir) => {
      val source = createDF(50000, spark).toDF().coalesce(4)

      writeTestData(source, Seq("a", "c"), 4000, tmpdir)

      val indexed = spark.read.format("qbeast").load(tmpdir)

      indexed.where("a > 1").count shouldBe 49999
      indexed.where("a > 49999").count shouldBe 1

      indexed.where("a <= 1").count shouldBe 2
      indexed.where("a <= 49999").count shouldBe 50000

    })

}
