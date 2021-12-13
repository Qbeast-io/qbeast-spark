package io.qbeast.spark.utils

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.{AllSpace, QbeastFile, Weight, WeightRange}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.{QueryIndexStatusExecutor, QuerySpec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

class QueryIndexStatusExecutorTest extends QbeastIntegrationTestSpec {

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

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 10)
      .save(tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val latestStatus = qbeastSnapshot.loadLatestIndexStatus
    val querySpec = QuerySpec(WeightRange(Weight(Int.MinValue), Weight(Int.MaxValue)), AllSpace())
    val queryRevisionExecutor =
      new QueryIndexStatusExecutor(querySpec, latestStatus)

    val allDeltaFiles = deltaLog.snapshot.allFiles.collect()
    val allFiles = allDeltaFiles.map(a => QbeastFile(a.path, a.tags))

    val matchFiles = queryRevisionExecutor.execute(allFiles)

    val diff = (allFiles.toSet -- matchFiles.toSet)

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })
}
