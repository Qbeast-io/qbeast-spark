package io.qbeast.spark.utils

import io.qbeast.core.model.Revision.stagingRevisionID
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.delta.DeltaLog

class QbeastDeltaStagingTest extends QbeastIntegrationTestSpec {
  it should "read a delta table as an entirely staged qbeast table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val df = loadTestData(spark).limit(10000)
      df.write.format("delta").save(tmpDir)

      val numStagingRows = spark.read.format("qbeast").load(tmpDir).count
      numStagingRows shouldBe 10000L
    })

  it should "not analyze or optimize the staging revision" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark).limit(10000)
      df.write.format("delta").save(tmpDir)

      val table = QbeastTable.forPath(spark, tmpDir)
      table.analyze(stagingRevisionID)
      table.optimize(stagingRevisionID)

      val allElements = spark.read.parquet(tmpDir).count
      allElements shouldBe 10000

      val snapshot = DeltaLog.forTable(spark, tmpDir).snapshot
      val qbeastSnapshot = DeltaQbeastSnapshot(snapshot)
      val stagingIndexStatus = qbeastSnapshot.loadIndexStatus(stagingRevisionID)

      stagingIndexStatus.replicatedOrAnnouncedSet.isEmpty shouldBe true
    })

  it should "correctly compact the staging revision" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.compact.minFileSize", "1")) { (spark, tmpDir) =>
    {
      val df = loadTestData(spark)

      df.repartition(20).write.format("delta").save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.snapshot
      val numFilesBefore = snapshot.numOfFiles

      val table = QbeastTable.forPath(spark, tmpDir)
      table.compact(stagingRevisionID)

      deltaLog.update()
      val numFilesAfter = deltaLog.snapshot.numOfFiles
      numFilesAfter shouldBe <(numFilesBefore)

      val deltaCount = spark.read.format("delta").load(tmpDir).count()
      val qbeastCount = spark.read.format("qbeast").load(tmpDir).count()

      deltaCount shouldBe 99986
      qbeastCount shouldBe 99986
    }
  }

  it should "sample a qbeast staging table correctly" in withSparkAndTmpDir((spark, tmpDir) => {
    val dataSize = 99986
    val df = loadTestData(spark) // 99986

    df.write.format("delta").save(tmpDir)
    val qdf = spark.read.format("qbeast").load(tmpDir)

    // We allow a 1% of tolerance in the sampling
    val tolerance = 0.01
    List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(f => {
      val result = qdf.sample(withReplacement = false, f).count().toDouble
      result shouldBe (dataSize * f) +- dataSize * f * tolerance
    })
  })

}
