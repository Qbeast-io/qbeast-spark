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
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T2
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
      val qbeastSnapshot = getQbeastSnapshot(tmpDir)

      qbeastSnapshot.loadAllRevisions.size shouldBe 2
      qbeastSnapshot.existsRevision(stagingID)
    })

  it should "be readable using both formats after Analyze and Optimize" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      writeHybridTable(spark, tmpDir)

      // Optimize the staging revision
      QbeastTable.forPath(spark, tmpDir).optimize(stagingID)

      // DataFrame should not change by optimizing the staging revision
      val qbeastDf = spark.read.format("qbeast").load(tmpDir)
      val deltaDf = spark.read.format("delta").load(tmpDir)
      qbeastDf.count() shouldBe deltaDf.count()

      assertLargeDatasetEquality(qbeastDf, deltaDf)

      // Should preserve standing staging revision behavior
      val qbeastSnapshot = getQbeastSnapshot(tmpDir)
      val stagingIndexStatus = qbeastSnapshot.loadIndexStatus(stagingID)
      stagingIndexStatus.cubesStatuses.size shouldBe 1
      stagingIndexStatus.replicatedOrAnnouncedSet.isEmpty shouldBe true
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
