package io.qbeast.spark.utils

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.StagingUtils
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

class QbeastDeltaStagingTest extends QbeastIntegrationTestSpec with StagingUtils {
  val columnsToIndex: Seq[String] = Seq("a", "b")
  val qDataSize = 10000
  val dDataSize = 10000
  val totalSize: Long = qDataSize + dDataSize
  val numSparkPartitions = 20

  def writeHybridTable(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._

    // Write qbeast data
    val qdf = (0 until qDataSize).map(i => T2(i, i)).toDF("a", "b")
    qdf.write
      .format("qbeast")
      .option("columnsToIndex", columnsToIndex.mkString(","))
      .option("cubeSize", "5000")
      .save(dir)

    // Create hybrid table by appending delta data
    val ddf = (qDataSize until qDataSize + dDataSize).map(i => T2(i, i)).toDF("a", "b")
    ddf
      .repartition(numSparkPartitions)
      .write
      .mode("append")
      .format("delta")
      .save(dir)
  }

  "A qbeast + delta hybrid table" should "be read correctly" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      writeHybridTable(spark, tmpDir)

      val qbeastDf = spark.read.format("qbeast").load(tmpDir)
      val deltaDf = spark.read.format("delta").load(tmpDir)
      assertLargeDatasetEquality(qbeastDf, deltaDf)

      // Should have the staging revision and the first revision
      val snapshot = DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot
      val qs = DeltaQbeastSnapshot(snapshot)
      qs.loadAllRevisions.size shouldBe 2
      qs.existsRevision(stagingID)
    })

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
