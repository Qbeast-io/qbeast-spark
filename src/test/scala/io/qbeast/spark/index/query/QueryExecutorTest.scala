package io.qbeast.spark.index.query

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.{CubeId, QbeastBlock, Weight, WeightRange}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.catalyst.expressions.{
  And,
  Expression,
  GreaterThanOrEqual,
  LessThan,
  Literal
}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Column, SparkSession}

class QueryExecutorTest extends QbeastIntegrationTestSpec {

  private def createDF(size: Int, spark: SparkSession) = {
    import spark.implicits._

    0.to(size)
      .map(i => T2(i, i.toDouble))
      .toDF()
      .as[T2]

  }

  private def weightFilters(weightRange: WeightRange): Expression = {
    val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("a").expr, new Column("c").expr))
    val lessThan = LessThan(qbeast_hash, Literal(weightRange.to.value))
    val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(weightRange.from.value))
    And(lessThan, greaterThanOrEqual)
  }

  behavior of "QueryExecutor"

  it should "find all sample files" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.path)

    val diff = allFiles.toSet -- matchFiles.toSet

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "filter the files according to the weight" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val filters = Seq(weightFilters(WeightRange(Weight.MinValue, Weight(0.001))))
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.path)

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "filter the files according to the space" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val filters = Seq(expr("a >= 2 and a < 10").expr)
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.path)

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
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    qbeastSnapshot.loadAllRevisions.size shouldBe 2

    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "skip files with maxWeight < weightRange.from" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = createDF(500000, spark).toDF()

      writeTestData(df, Seq("a", "c"), 10000, tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

      val weightRange = WeightRange(Weight(3), Weight(5))
      val expressionFilters = weightFilters(weightRange)
      val querySpecBuilder = new QuerySpecBuilder(Seq(expressionFilters))

      val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)

      val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
      val allFiles = allDeltaFiles.map(_.path)

      val matchFiles = queryExecutor.execute().map(_.path)
      val diff = allFiles.toSet -- matchFiles.toSet

      // scalastyle:off println
      println(
        s"Number of files: ${allFiles.length}, Matching files: ${matchFiles.length}, " +
          s"Skipped files: ${diff.size}")

      val allQbeastFiles = allDeltaFiles.map(addFile =>
        QbeastBlock(addFile.path, addFile.tags, addFile.size, addFile.modificationTime))

      for (f <- allQbeastFiles) {
        if (f.maxWeight < weightRange.from) {
          diff should contain(f.path)
          matchFiles should not contain (f.path)
        }
      }
    })

  it should "handle index with a missing inner cube" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)

    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
    val revision = qbeastSnapshot.loadAllRevisions.head
    val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)

    val innerCubesLevel1 =
      indexStatus.cubesStatuses.keys.filter(cube =>
        cube.depth == 1 && indexStatus.cubesStatuses.contains(cube))
    val cubeToRemove = if (innerCubesLevel1.isEmpty) CubeId.root(2) else innerCubesLevel1.head

    val faultyIndexStatus =
      indexStatus.copy(cubesStatuses = indexStatus.cubesStatuses - cubeToRemove)

    val querySpecBuilder = new QuerySpecBuilder(Seq.empty)
    val querySpec = querySpecBuilder.build(revision)
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    val matchFiles = queryExecutor
      .executeRevision(querySpec, faultyIndexStatus)
      .map(_.path)

    val allFiles = deltaLog.snapshot.allFiles.collect().map(_.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 1
  })
}
