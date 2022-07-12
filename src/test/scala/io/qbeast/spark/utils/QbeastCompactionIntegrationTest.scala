package io.qbeast.spark.utils

import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._

class QbeastCompactionIntegrationTest extends QbeastIntegrationTestSpec {

  private def writeTestDataInBatches(batch: DataFrame, tmpDir: String, numBatches: Int): Unit = {
    1.to(numBatches).foreach { _ =>
      writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir, "append")
    }
  }

  "Compaction command" should
    "reduce the number of files" in withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.compact.minFileSize", "1")) { (spark, tmpDir) =>
      {

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

      }
    }

  it should
    "compact in more than one file if MAX_FILE_SIZE_COMPACTION " +
    "is exceeded" in withExtendedSparkAndTmpDir(
      new SparkConf()
        .set("spark.qbeast.compact.minFileSize", "1")
        .set("spark.qbeast.compact.maxFileSize", "2000000")) { (spark, tmpDir) =>
      {

        val data = loadTestData(spark)

        // Creating four batches of 20000 elements each one
        // So they all go to the root cube
        // and we can compact them later
        val batch = data.limit(20000)

        // Write four batches
        writeTestDataInBatches(batch, tmpDir, 4)

        val originalNumOfFilesRoot =
          DeltaLog.forTable(spark, tmpDir).snapshot.allFiles.filter("tags.cube == ''").count()

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        val finalNumOfFilesRoot =
          DeltaLog.forTable(spark, tmpDir).snapshot.allFiles.filter("tags.cube == ''").count()

        finalNumOfFilesRoot shouldBe >(1L)
        finalNumOfFilesRoot shouldBe <(originalNumOfFilesRoot)

      }
    }

  it should "not compact anything if sizes already match" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        // Write four batches
        writeTestDataInBatches(data, tmpDir, 2)

        val originalNumOfFilesRoot =
          DeltaLog.forTable(spark, tmpDir).snapshot.allFiles.filter("tags.cube == ''").count()

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        val finalNumOfFilesRoot =
          DeltaLog.forTable(spark, tmpDir).snapshot.allFiles.filter("tags.cube == ''").count()

        finalNumOfFilesRoot shouldBe originalNumOfFilesRoot
      }
  }

  it should "respect cube information" in withExtendedSparkAndTmpDir(
    new SparkConf().set("spark.qbeast.compact.minFileSize", "1"))((spark, tmpDir) => {

    val data = loadTestData(spark)

    // Write four batches
    writeTestDataInBatches(data, tmpDir, 4)

    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val originalIndexStatus = DeltaQbeastSnapshot(deltaLog.snapshot).loadLatestIndexStatus

    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact()

    val newIndexStatus = DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

    newIndexStatus.revision shouldBe originalIndexStatus.revision
    originalIndexStatus.cubeNormalizedWeights.foreach { case (cube, weight) =>
      newIndexStatus.cubeNormalizedWeights.get(cube) shouldBe defined
      newIndexStatus.cubeNormalizedWeights(cube) shouldBe weight
    }
    newIndexStatus.replicatedSet shouldBe originalIndexStatus.replicatedSet
    newIndexStatus.announcedSet shouldBe originalIndexStatus.announcedSet
  })

}