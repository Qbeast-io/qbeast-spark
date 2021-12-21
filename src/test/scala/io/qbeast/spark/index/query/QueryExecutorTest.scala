package io.qbeast.spark.index.query

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.{QbeastFile, Weight, WeightRange}
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

  behavior of "QueryExecutor"

  it should "find all sample files" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(1000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 10, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(a => QbeastFile(a.path, a.tags))

    val matchFiles = queryExecutor.execute(allFiles)

    val diff = (allFiles.toSet -- matchFiles.toSet)

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "filter the files according to the weight" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(1000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 10, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    def weightFilters(weightRange: WeightRange): Expression = {
      val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("a").expr, new Column("c").expr))
      val lessThan = LessThan(qbeast_hash, Literal(weightRange.to.value))
      val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(weightRange.from.value))
      And(lessThan, greaterThanOrEqual)
    }
    val filters = Seq(weightFilters(WeightRange(Weight.MinValue, Weight(0.001))))
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(a => QbeastFile(a.path, a.tags))

    val matchFiles = queryExecutor.execute(allFiles)

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "filter the files according to the space" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(1000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 10, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val filters = Seq(expr("a >= 2 and a < 10").expr)
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(a => QbeastFile(a.path, a.tags))

    val matchFiles = queryExecutor.execute(allFiles)

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "find all files in different revisions" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(1000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 10, tmpdir)

    val differentRevision =
      source
        .withColumn("a", col("a") * 5)
        .withColumn("c", col("c") * 6)

    writeTestData(differentRevision, Seq("a", "c"), 10, tmpdir, "append")

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    qbeastSnapshot.loadAllRevisions.size shouldBe 2

    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(a => QbeastFile(a.path, a.tags))

    val matchFiles = queryExecutor.execute(allFiles)

    val diff = (allFiles.toSet -- matchFiles.toSet)
    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })
}
