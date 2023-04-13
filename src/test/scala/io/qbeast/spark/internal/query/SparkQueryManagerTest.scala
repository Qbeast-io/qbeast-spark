package io.qbeast.spark.internal.query

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

class SparkQueryManagerTest extends QbeastIntegrationTestSpec {

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

  behavior of "SparkQueryManager"

  it should "find all sample files" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
    val filters = Seq.empty

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = SparkQueryManager.query(filters, qbeastSnapshot).map(_.path)

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

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = SparkQueryManager.query(filters, qbeastSnapshot).map(_.path)

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "filter the files according to the space" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val filters = Seq(expr("a >= 2 and a < 10").expr)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = SparkQueryManager.query(filters, qbeastSnapshot).map(_.path)

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

    // Including the staging revision
    qbeastSnapshot.loadAllRevisions.size shouldBe 3

    val filters = Seq.empty
    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = SparkQueryManager.query(filters, qbeastSnapshot).map(_.path)

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

      val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
      val allFiles = allDeltaFiles.map(_.path)

      val matchFiles = SparkQueryManager.query(Seq(expressionFilters), qbeastSnapshot).map(_.path)
      val diff = allFiles.toSet -- matchFiles.toSet

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
    val revision = qbeastSnapshot.loadLatestRevision
    val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)

    val innerCubesLevel1 =
      indexStatus.cubesStatuses.keys.filter(cube =>
        cube.depth == 1 && indexStatus.cubesStatuses.contains(cube))
    val cubeToRemove = if (innerCubesLevel1.isEmpty) CubeId.root(2) else innerCubesLevel1.head

    val faultyIndexStatus =
      indexStatus.copy(cubesStatuses = indexStatus.cubesStatuses - cubeToRemove)

    val querySpec = SparkQueryManager.buildSpec(Seq.empty, revision)

    val matchFiles = SparkQueryManager.queryRevision(querySpec, faultyIndexStatus).map(_.path)

    val allFiles = deltaLog.snapshot.allFiles.collect().map(_.path)

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
