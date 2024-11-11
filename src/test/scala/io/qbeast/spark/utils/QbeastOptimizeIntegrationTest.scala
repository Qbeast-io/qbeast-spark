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

import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.QTableID
import io.qbeast.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.delta.DeltaMetadataManager
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

class QbeastOptimizeIntegrationTest extends QbeastIntegrationTestSpec {

  def createTableWithMultipleAppends(spark: SparkSession, tmpDir: String): Unit = {
    val options = Map(
      "columnsToIndex" -> "col_1,col_2",
      "cubeSize" -> "100",
      "columnStats" ->
        """{"col_1_min": 0.0, "col_1_max": 5000.0, "col_2_min": 0.0, "col_2_max": 5000.0}""")
    spark
      .range(5000)
      .withColumn("col_1", rand() % 5000)
      .withColumn("col_2", rand() % 5000)
      .write
      .format("qbeast")
      .options(options)
      .save(tmpDir)
    spark
      .range(5000)
      .withColumn("col_1", rand() % 5000)
      .withColumn("col_2", rand() % 5000)
      .write
      .mode("append")
      .format("qbeast")
      .save(tmpDir)
  }

  behavior of "A fully optimized index"

  it should "have no cube fragmentation" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    createTableWithMultipleAppends(spark, tmpDir)
    val qt = QbeastTable.forPath(spark, tmpDir)
    val elementCountBefore = qt.getIndexMetrics.elementCount
    qt.optimize()

    val mAfter = qt.getIndexMetrics
    val fragmentationAfter = mAfter.blockCount / mAfter.cubeCount.toDouble
    val elementCountAfter = mAfter.elementCount

    fragmentationAfter shouldBe 1d
    elementCountBefore shouldBe elementCountAfter
  }

  it should "sample correctly" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    createTableWithMultipleAppends(spark, tmpDir)
    val df = spark.read.format("qbeast").load(tmpDir)
    val dataSize = df.count()

    val qt = QbeastTable.forPath(spark, tmpDir)
    qt.optimize()

    // Here, we use a tolerance of 5% because the total number of elements is relatively small
    val tolerance = 0.05
    List(0.1, 0.2, 0.5, 0.7, 0.99).foreach { f =>
      val margin = dataSize * f * tolerance
      val sampleSize = df.sample(f).count().toDouble
      sampleSize shouldBe (dataSize * f) +- margin
    }
  }

  "Optimizing with given fraction" should "improve sampling efficiency" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      def getSampledFiles(fraction: Double): Seq[IndexFile] = {
        val qs = getQbeastSnapshot(tmpDir)
        qs.loadLatestIndexFiles
          .filter(f => f.blocks.exists(_.minWeight.fraction <= fraction))
          .collect()
      }

      createTableWithMultipleAppends(spark, tmpDir)
      val fraction: Double = 0.1
      val filesBefore = getSampledFiles(fraction)

      QbeastTable.forPath(spark, tmpDir).optimize(fraction)

      val filesAfter = getSampledFiles(fraction)
      // We should be reading fewer files
      filesAfter.size should be < filesBefore.size
      // We should be reading fewer data
      filesAfter.map(_.size).sum should be < filesBefore.map(_.size).sum
      // We should be reading fewer blocks
      filesAfter.map(_.blocks.size).sum should be < filesBefore.map(_.blocks.size).sum
  }

  "Table optimize" should "set the dataChange flag as false" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val df = spark.sparkContext.range(0, 10).toDF("id")
      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .save(tmpDir)

      QbeastTable.forPath(spark, tmpDir).optimize(1L, Map.empty[String, String])

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .collect({
          case addFile: AddFile => addFile.dataChange shouldBe false
          case removeFile: RemoveFile => removeFile.dataChange shouldBe false
          case commitInfo: CommitInfo =>
            commitInfo.isolationLevel shouldBe Some("SnapshotIsolation")
          case _ => None
        })

    }

  /**
   * Get the unindexed files from the last updated Snapshot
   * @param deltaLog
   * @return
   */
  def getUnindexedFilesFromDelta(qtableID: QTableID): Dataset[IndexFile] = {
    DeltaMetadataManager.loadSnapshot(qtableID).loadIndexFiles(0L) // Revision 0L
  }

  /**
   * Get the indexed files from the last updated Snapshot
   * @param deltaLog
   * @return
   */
  def getIndexedFilesFromDelta(qtableID: QTableID): Dataset[IndexFile] = {
    DeltaMetadataManager.loadSnapshot(qtableID).loadLatestIndexFiles
  }

  def getAllFilesFromDelta(spark: SparkSession, d: QTableID): Dataset[AddFile] = {
    DeltaLog.forTable(spark, d.id).update().allFiles
  }

  def checkLatestRevisionAfterOptimize(spark: SparkSession, qTableID: QTableID): Unit = {
    // Check that the revision of the files is correct
    val indexedFiles = getIndexedFilesFromDelta(qTableID)
    val qbeastTable = QbeastTable.forPath(spark, qTableID.id)
    qbeastTable.allRevisions().size shouldBe 2L // 2 Revisions: 0L and 1L
    qbeastTable.latestRevisionID shouldBe 1L
    qbeastTable.indexedColumns() shouldBe Seq("id")
    indexedFiles
      .select("revisionId")
      .distinct()
      .count() shouldBe 1L // 1 Revision
    indexedFiles
      .select("revisionId")
      .head()
      .getLong(0) shouldBe 1L // The latest Revision
  }

  "Optimizing the Revision 0L" should "optimize a table converted to Qbeast" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      spark
        .range(50)
        .write
        .mode("append")
        .format("delta")
        .save(tmpDir) // Append data without indexing

      ConvertToQbeastCommand(identifier = s"delta.`$tmpDir`", columnsToIndex = Seq("id"))
        .run(spark)

      val qtableID = QTableID(tmpDir)
      val firstUnindexedFiles = getUnindexedFilesFromDelta(qtableID)
      val allFiles = getAllFilesFromDelta(spark, qtableID)
      firstUnindexedFiles.count() shouldBe allFiles.count()
      // Optimize the Table
      val qt = QbeastTable.forPath(spark, tmpDir)
      qt.optimize(0L)

      // After optimization, all files from the Legacy Table should be indexed
      val unindexedFiles = getUnindexedFilesFromDelta(qtableID)
      unindexedFiles shouldBe empty
      // Check that the indexed files are correct
      val indexedFiles = getIndexedFilesFromDelta(qtableID)
      val allFilesAfter = getAllFilesFromDelta(spark, qtableID)
      indexedFiles.count() shouldBe allFilesAfter.count()

      checkLatestRevisionAfterOptimize(spark, qtableID)

  }

  it should "optimize and Hybrid Table" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    spark
      .range(50)
      .write
      .mode("append")
      .option("columnsToIndex", "id")
      .format("qbeast")
      .save(tmpDir)

    spark
      .range(50)
      .write
      .mode("append")
      .format("delta")
      .save(tmpDir) // Append data without indexing

    val qtableID = QTableID(tmpDir)
    val firstUnindexedFiles = getUnindexedFilesFromDelta(qtableID)
    firstUnindexedFiles should not be empty

    // Optimize the Table
    val qt = QbeastTable.forPath(spark, tmpDir)
    qt.optimize(0L)

    // After optimization, all files from the Hybrid Table should be indexed
    val unindexedFiles = getUnindexedFilesFromDelta(qtableID)
    unindexedFiles shouldBe empty

    // Check that the revision is correct
    checkLatestRevisionAfterOptimize(spark, qtableID)
  }

  it should "Optimize a fraction of the Staging Area" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      // Index with Qbeast
      spark
        .range(50)
        .write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .save(tmpDir)

      spark
        .range(100)
        .write
        .mode("append")
        .format("delta")
        .save(tmpDir) // Append data without indexing

      // Check that the number of unindexed files is not 0
      val qtableID = QTableID(tmpDir)
      val unindexedFilesBefore = getUnindexedFilesFromDelta(qtableID)
      val unindexedFilesCount = unindexedFilesBefore.count()
      unindexedFilesCount should be > 0L
      val unindexedFilesSize = unindexedFilesBefore.collect().map(_.size).sum

      // Optimize the Table with a 0.5 fraction
      val qt = QbeastTable.forPath(spark, tmpDir)
      val fractionToOptimize = 0.5
      qt.optimize(revisionID = 0L, fraction = fractionToOptimize)

      // After optimization, half of the Staging Area should be indexed
      val unindexedFilesAfter = getUnindexedFilesFromDelta(qtableID)
      // Not all files should be indexed
      unindexedFilesAfter should not be empty
      // The number of unindexed files should be less than the original number
      unindexedFilesAfter.count() shouldBe <(unindexedFilesCount)
      // The size of the unindexed files should be less or equal than the missing fraction to optimize
      val unindexedFilesSizeAfter = unindexedFilesAfter.collect().map(_.size).sum
      unindexedFilesSizeAfter shouldBe >(0L)
      unindexedFilesSizeAfter shouldBe <=(
        ((1.0 - fractionToOptimize) * unindexedFilesSize).toLong)

      // Second optimization should index the rest of the Staging Area
      qt.optimize(revisionID = 0L, fraction = 1.0)
      val unindexedFiles2 = getUnindexedFilesFromDelta(qtableID)
      unindexedFiles2 shouldBe empty
  }

}
