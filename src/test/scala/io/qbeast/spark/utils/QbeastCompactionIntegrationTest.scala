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

import io.qbeast.core.model.QTableID
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.SnapshotIsolation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class QbeastCompactionIntegrationTest extends QbeastIntegrationTestSpec with Logging {

  private def writeTestDataInBatches(batch: DataFrame, tmpDir: String, numBatches: Int): Unit = {
    1.to(numBatches).foreach { _ =>
      writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir, "append")
    }
  }

  "Compaction command" should
    "reduce the number of files" in withSparkAndTmpDir((spark, tmpDir) => {

      val data = loadTestData(spark)

      // Creating four batches of 20000 elements each one
      // So they all go to the root cube
      // and we can compact them later
      val limit = 20000
      val numBatches = 4
      val batch = data.limit(limit)

      // Write four batches
      writeTestDataInBatches(batch, tmpDir, numBatches)

      val indexed = spark.read.format("qbeast").load(tmpDir)
      val originalNumOfFiles = indexed.select(input_file_name()).distinct().count()

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable.compact()

      val finalNumOfFiles = indexed.select(input_file_name()).distinct().count()
      finalNumOfFiles shouldBe <(originalNumOfFiles)
      finalNumOfFiles shouldBe 1L

      // Test if the dataframe is correctly loaded
      val deltaData = spark.read.format("delta").load(tmpDir)
      indexed.count() shouldBe (limit * numBatches)
      assertLargeDatasetEquality(indexed, deltaData, orderedComparison = false)
    })

  it should
    "compact in more than one file if MAX_FILE_SIZE_COMPACTION " +
    "is exceeded" in withSparkAndTmpDir((spark, tmpDir) => {

      val data = loadTestData(spark)

      // Creating four batches of 20000 elements each one
      // So they all go to the root cube
      // and we can compact them later
      val batch = data.limit(20000)

      // Write four batches
      writeTestDataInBatches(batch, tmpDir, 4)

      val originalNumOfFilesRoot = getRootCubeFileCount(spark, tmpDir)

      // Compact the tables
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable.compact()

      // Check if number of files are less than the original
      val finalNumOfFilesRoot = getRootCubeFileCount(spark, tmpDir)

      finalNumOfFilesRoot shouldBe >=(1L)
      finalNumOfFilesRoot shouldBe <=(originalNumOfFilesRoot)

    })

  it should "respect cube information" in withSparkAndTmpDir((spark, tmpDir) => {

    val data = loadTestData(spark)

    // Write four batches
    writeTestDataInBatches(data, tmpDir, 4)

    // Load the index status before manipulating the files
    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val originalIndexStatus =
      DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

    // Compact the table
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact()

    val newIndexStatus = DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

    // Check if both index status are coherent with each other
    newIndexStatus.revision shouldBe originalIndexStatus.revision
    originalIndexStatus.cubesStatuses.foreach { case (cube, status) =>
      newIndexStatus.cubesStatuses.get(cube) shouldBe defined
      newIndexStatus.cubesStatuses(cube).maxWeight shouldBe status.maxWeight
    }
    newIndexStatus.replicatedSet shouldBe originalIndexStatus.replicatedSet
    newIndexStatus.announcedSet shouldBe originalIndexStatus.announcedSet
  })

  it should "compact the latest revision available" in withSparkAndTmpDir((spark, tmpDir) => {

    val data = loadTestData(spark)

    // Write four batches
    writeTestDataInBatches(data, tmpDir, 4)

    // Write next revision batches
    val newData = data
      .withColumn("product_id", col("product_id") * 2)
      .withColumn("user_id", col("user_id") * 6)
    writeTestDataInBatches(newData, tmpDir, 4)

    val tableId = QTableID(tmpDir)
    // Including the staging revision
    SparkDeltaMetadataManager.loadSnapshot(tableId).loadAllRevisions.size shouldBe 3

    // Count files written for each revision
    val allFiles = DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.allFiles
    val originalFilesRevisionOne =
      allFiles.filter("tags.revision == 1").count()
    val originalFilesRevisionTwo =
      allFiles.filter("tags.revision == 2").count()

    // Compact the table
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact()

    // Count files compacted for each revision
    val newAllFiles = DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.allFiles
    val newFilesRevisionOne = newAllFiles.filter("tags.revision == 1").count()
    val newFilesRevisionTwo = newAllFiles.filter("tags.revision == 2").count()

    // Check if the compaction worked for the latest one
    newFilesRevisionOne shouldBe originalFilesRevisionOne
    newFilesRevisionTwo shouldBe <(originalFilesRevisionTwo)

  })

  it should "compact the specified revision" in withSparkAndTmpDir((spark, tmpDir) => {

    val data = loadTestData(spark)

    // Write four batches
    writeTestDataInBatches(data, tmpDir, 4)

    // Write next revision batches
    val newData = data
      .withColumn("product_id", col("product_id") * 2)
      .withColumn("user_id", col("user_id") * 6)
    writeTestDataInBatches(newData, tmpDir, 4)

    val tableId = QTableID(tmpDir)
    // Including the staging revision
    SparkDeltaMetadataManager.loadSnapshot(tableId).loadAllRevisions.size shouldBe 3

    // Count files written for each revision
    val allFiles = DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.allFiles
    val originalFilesRevisionOne =
      allFiles.filter("tags.revision == 1").count()
    val originalFilesRevisionTwo =
      allFiles.filter("tags.revision == 2").count()

    // Compact the table
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact(1)

    // Count files compacted for each revision
    val newAllFiles = DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.allFiles
    val newFilesRevisionOne = newAllFiles.filter("tags.revision == 1").count()
    val newFilesRevisionTwo = newAllFiles.filter("tags.revision == 2").count()

    // Check if the compaction worked for the number one
    newFilesRevisionOne shouldBe <(originalFilesRevisionOne)
    newFilesRevisionTwo shouldBe originalFilesRevisionTwo

  })

  it should "not compact if the revision does not exists" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      val data = loadTestData(spark)

      // Write four batches
      writeTestDataInBatches(data, tmpDir, 4)

      // Try to compact the table with non-existing revision ID
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      a[AnalysisException] shouldBe thrownBy(qbeastTable.compact(3))

    })

  private def getRootCubeFileCount(spark: SparkSession, directory: String): Long = {
    val deltaLog = DeltaLog.forTable(spark, directory)
    val snapshot = DeltaQbeastSnapshot(deltaLog.unsafeVolatileSnapshot)
    snapshot.loadLatestIndexFiles.size
  }

  "An optimization execution" should "not change data and use SnapshotIsolation" in
    withQbeastContextSparkAndTmpDir((spark, tmpDir) => {
      // This test makes sure that FileActions from optimization have 'dataChange=false'
      // and the isolation level is set to SnapshotIsolation to pass the following check:
      // checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn.
      // This is to ensure that Optimization processes can be executed concurrently with
      // other transactions.
      val data = loadTestData(spark).limit(100)
      writeTestData(data, Seq("user_id", "product_id"), 100, tmpDir)

      val qt = QbeastTable.forPath(spark, tmpDir)
      val m = qt.getIndexMetrics()
      val filePath = m.cubeStatuses.values.flatMap(_.blocks.map(_.filePath)).head
      qt.optimize(Seq(filePath))

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .foreach {
          case commitInfo: CommitInfo =>
            // It should be using the Snapshot Isolation level
            commitInfo.isolationLevel shouldBe Some(SnapshotIsolation.toString())
          case add: AddFile => add.dataChange shouldBe false
          case remove: RemoveFile => remove.dataChange shouldBe false
          case _ =>
        }
    })

}
