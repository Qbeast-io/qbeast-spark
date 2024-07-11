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
package io.qbeast.spark.utils

import io.qbeast.core.model.StagingUtils
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import io.qbeast.TestClasses.T2
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.SparkSession

class QbeastDeltaStagingTest extends QbeastIntegrationTestSpec with StagingUtils {
  val columnsToIndex: Seq[String] = Seq("a", "b")
  val qDataSize = 10000
  val dDataSize = 10000
  val totalSize: Long = qDataSize + dDataSize
  val numSparkPartitions = 20

  def writeHybridTable(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._

    // Write qbeast data
    val qdf = spark.range(qDataSize).map(i => T2(i, i.toDouble)).toDF("a", "b")
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

  it should "be readable using both formats after Analyze and Optimize" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      writeHybridTable(spark, tmpDir)

      // Analyze and Optimize the staging revision
      val table = QbeastTable.forPath(spark, tmpDir)
      table.analyze(stagingID)
      table.optimize(stagingID, Map.empty[String, String])

      // DataFrame should not change by optimizing the staging revision
      val qbeastDf = spark.read.format("qbeast").load(tmpDir)
      val deltaDf = spark.read.format("delta").load(tmpDir)
      qbeastDf.count() shouldBe deltaDf.count()

      assertLargeDatasetEquality(qbeastDf, deltaDf)

      // Should preserve standing staging revision behavior
      val snapshot = DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot
      val qbeastSnapshot = DeltaQbeastSnapshot(snapshot)
      val stagingIndexStatus = qbeastSnapshot.loadIndexStatus(stagingID)
      stagingIndexStatus.cubesStatuses.size shouldBe 1
      stagingIndexStatus.replicatedOrAnnouncedSet.isEmpty shouldBe true
    })

  it should "correctly compact the staging revision" in withSparkAndTmpDir((spark, tmpDir) => {
    writeHybridTable(spark, tmpDir)

    // Number of delta files before compaction
    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val qsBefore = DeltaQbeastSnapshot(deltaLog.update())
    val numFilesBefore = qsBefore.loadIndexFiles(stagingID).count()

    // Perform compaction
    val table = QbeastTable.forPath(spark, tmpDir)
    table.compact(stagingID, Map.empty[String, String])

    // Number of delta files after compaction
    val qsAfter = DeltaQbeastSnapshot(deltaLog.update())
    val numFilesAfter = qsAfter.loadIndexFiles(stagingID).count()

    numFilesAfter shouldBe <(numFilesBefore)

    val deltaDf = spark.read.format("delta").load(tmpDir)
    val qbeastDf = spark.read.format("qbeast").load(tmpDir)

    assertLargeDatasetEquality(qbeastDf, deltaDf)
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
