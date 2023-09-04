package io.qbeast.spark.utils

import io.qbeast.core.model.QTableID
import io.qbeast.spark.delta.{DeltaQbeastSnapshot, SparkDeltaMetadataManager}
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._

class QbeastCompactionIntegrationTest extends QbeastIntegrationTestSpec {

  private def writeTestDataInBatches(batch: DataFrame, tmpDir: String, numBatches: Int): Unit = {
    1.to(numBatches).foreach { _ =>
      writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir, "append")
    }
  }

  "Compaction command" should
    "reduce the number of files" ignore withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog.set("spark.qbeast.compact.minFileSizeInBytes", "1")) {
      (spark, tmpDir) =>
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
    "is exceeded" ignore withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.qbeast.compact.minFileSizeInBytes", "1")
        .set("spark.qbeast.compact.maxFileSizeInBytes", "2000000")) { (spark, tmpDir) =>
      {

        val data = loadTestData(spark)

        // Creating four batches of 20000 elements each one
        // So they all go to the root cube
        // and we can compact them later
        val batch = data.limit(20000)

        // Write four batches
        writeTestDataInBatches(batch, tmpDir, 4)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val originalNumOfFilesRoot =
          deltaLog.snapshot.allFiles.filter("tags.cube == ''").count()

        // Compact the tables
        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        // Check if number of files are less than the original
        val finalNumOfFilesRoot =
          deltaLog.update().allFiles.filter("tags.cube == ''").count()

        finalNumOfFilesRoot shouldBe >(1L)
        finalNumOfFilesRoot shouldBe <(originalNumOfFilesRoot)

      }
    }

  it should "not compact anything if sizes already match" ignore withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        // Write four batches
        writeTestDataInBatches(data, tmpDir, 2)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val originalNumOfFilesRoot =
          deltaLog.snapshot.allFiles.filter("tags.cube == ''").count()

        // Compact the table
        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        val finalNumOfFilesRoot =
          deltaLog.update().allFiles.filter("tags.cube == ''").count()

        finalNumOfFilesRoot shouldBe originalNumOfFilesRoot
      }
  }

  it should "respect cube information" ignore withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.compact.minFileSizeInBytes", "1"))((spark, tmpDir) => {

    val data = loadTestData(spark)

    // Write four batches
    writeTestDataInBatches(data, tmpDir, 4)

    // Load the index status before manipulating the files
    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val originalIndexStatus = DeltaQbeastSnapshot(deltaLog.snapshot).loadLatestIndexStatus

    // Compact the table
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact()

    val newIndexStatus = DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

    // Check if both index status are coherent with each other
    newIndexStatus.revision shouldBe originalIndexStatus.revision
    originalIndexStatus.cubeNormalizedWeights.foreach { case (cube, weight) =>
      newIndexStatus.cubeNormalizedWeights.get(cube) shouldBe defined
      newIndexStatus.cubeNormalizedWeights(cube) shouldBe weight
    }
    newIndexStatus.replicatedSet shouldBe originalIndexStatus.replicatedSet
    newIndexStatus.announcedSet shouldBe originalIndexStatus.announcedSet
  })

  it should "compact the latest revision available" ignore withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.compact.minFileSizeInBytes", "1"))((spark, tmpDir) => {

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
    val allFiles = DeltaLog.forTable(spark, tmpDir).snapshot.allFiles
    val originalFilesRevisionOne =
      allFiles.filter("tags.revision == 1").count()
    val originalFilesRevisionTwo =
      allFiles.filter("tags.revision == 2").count()

    // Compact the table
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact()

    // Count files compacted for each revision
    val newAllFiles = DeltaLog.forTable(spark, tmpDir).snapshot.allFiles
    val newFilesRevisionOne = newAllFiles.filter("tags.revision == 1").count()
    val newFilesRevisionTwo = newAllFiles.filter("tags.revision == 2").count()

    // Check if the compaction worked for the latest one
    newFilesRevisionOne shouldBe originalFilesRevisionOne
    newFilesRevisionTwo shouldBe <(originalFilesRevisionTwo)

  })

  it should "compact the specified revision" ignore withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.compact.minFileSizeInBytes", "1"))((spark, tmpDir) => {

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
    val allFiles = DeltaLog.forTable(spark, tmpDir).snapshot.allFiles
    val originalFilesRevisionOne =
      allFiles.filter("tags.revision == 1").count()
    val originalFilesRevisionTwo =
      allFiles.filter("tags.revision == 2").count()

    // Compact the table
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.compact(1)

    // Count files compacted for each revision
    val newAllFiles = DeltaLog.forTable(spark, tmpDir).snapshot.allFiles
    val newFilesRevisionOne = newAllFiles.filter("tags.revision == 1").count()
    val newFilesRevisionTwo = newAllFiles.filter("tags.revision == 2").count()

    // Check if the compaction worked for the number one
    newFilesRevisionOne shouldBe <(originalFilesRevisionOne)
    newFilesRevisionTwo shouldBe originalFilesRevisionTwo

  })

  it should "not compact if the revision does not exists" ignore withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.compact.minFileSize", "1"))((spark, tmpDir) => {

    val data = loadTestData(spark)

    // Write four batches
    writeTestDataInBatches(data, tmpDir, 4)

    // Try to compact the table with non-existing revision ID
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    a[AnalysisException] shouldBe thrownBy(qbeastTable.compact(3))

  })
}
