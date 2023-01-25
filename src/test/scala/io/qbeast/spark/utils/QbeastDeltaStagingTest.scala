package io.qbeast.spark.utils

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.RevisionUtils.stagingID
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

class QbeastDeltaStagingTest extends QbeastIntegrationTestSpec {
  val columnsToIndex: Seq[String] = Seq("a", "b")
  val qDataSize = 10000
  val dDataSize = 10000
  val totalSize: Long = qDataSize + dDataSize
  val numSparkPartitions = 20

  def writeHybridTable(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._
    val qdf = (0 until qDataSize).map(i => T2(i, i)).toDF("a", "b")
    qdf.write
      .format("qbeast")
      .option("columnsToIndex", columnsToIndex.mkString(","))
      .option("cubeSize", "5000")
      .save(dir)

    val ddf = (qDataSize until qDataSize + dDataSize).map(i => T2(i, i)).toDF("a", "b")
    ddf.repartition(numSparkPartitions).write.mode("append").format("delta").save(dir)
  }

  "A qbeast + delta hybrid table" should "be read correctly" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      writeHybridTable(spark, tmpDir)

      val numStagingRows = spark.read.format("qbeast").load(tmpDir).count
      numStagingRows shouldBe totalSize

      val snapshot = DeltaLog.forTable(spark, tmpDir).snapshot
      val qs = DeltaQbeastSnapshot(snapshot)
      qs.loadAllRevisions.size shouldBe 2
      qs.existsRevision(stagingID)
    })

  it should "not be altered by Optimizing the staging revision" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      writeHybridTable(spark, tmpDir)

      val table = QbeastTable.forPath(spark, tmpDir)
      table.analyze(stagingID)
      table.optimize(stagingID)

      val allElements = spark.read.parquet(tmpDir).count
      allElements shouldBe totalSize

      val snapshot = DeltaLog.forTable(spark, tmpDir).snapshot
      val qbeastSnapshot = DeltaQbeastSnapshot(snapshot)
      val stagingIndexStatus = qbeastSnapshot.loadIndexStatus(stagingID)

      stagingIndexStatus.cubesStatuses.size shouldBe 1
      stagingIndexStatus.replicatedOrAnnouncedSet.isEmpty shouldBe true
    })

  it should "correctly compact the staging revision" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.compact.minFileSize", "1")) { (spark, tmpDir) =>
    {
      writeHybridTable(spark, tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qsBefore = DeltaQbeastSnapshot(deltaLog.snapshot)
      val numFilesBefore = qsBefore.loadIndexStatus(stagingID).cubesStatuses.head._2.files.size

      val table = QbeastTable.forPath(spark, tmpDir)
      table.compact(stagingID)

      val qsAfter = DeltaQbeastSnapshot(deltaLog.update())
      val numFilesAfter = qsAfter.loadIndexStatus(stagingID).cubesStatuses.head._2.files.size
      numFilesAfter shouldBe <(numFilesBefore)

      val deltaCount = spark.read.format("delta").load(tmpDir).count()
      val qbeastCount = spark.read.format("qbeast").load(tmpDir).count()

      deltaCount shouldBe totalSize
      qbeastCount shouldBe totalSize
    }
  }

  it should "sample correctly" in withSparkAndTmpDir((spark, tmpDir) => {
    writeHybridTable(spark, tmpDir)
    val qdf = spark.read.format("qbeast").load(tmpDir)

    val tolerance = 0.05
    List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(f => {
      val sampleSize = qdf.sample(withReplacement = false, f).count().toDouble
      val margin = totalSize * f * tolerance

      sampleSize shouldBe (totalSize * f) +- margin
    })
  })

}
