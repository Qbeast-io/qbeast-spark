package io.qbeast.spark.index

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.{QTableID, StagingUtils}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.{DeltaQbeastSnapshot, StagingDataManager}
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester

class DataStagingTest
    extends QbeastIntegrationTestSpec
    with PrivateMethodTester
    with StagingUtils {

  def createDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    (0 until 10000).map(i => T2(i, i)).toDF()
  }

  def getQbeastSnapshot(spark: SparkSession, dir: String): DeltaQbeastSnapshot = {
    val deltaLog = DeltaLog.forTable(spark, dir)
    DeltaQbeastSnapshot(deltaLog.snapshot)
  }

  private val getCurrentStagingSize: PrivateMethod[Long] =
    PrivateMethod[Long]('currentStagingSize)

  "Data Staging" should "stage data during first write" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSizeInBytes", "1")) { (spark, tmpDir) =>
    {
      val df = createDF(spark)
      df.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "2000")
        .save(tmpDir)

      val revisions = getQbeastSnapshot(spark, tmpDir).loadAllRevisions
      revisions.size shouldBe 1
      isStaging(revisions.head) shouldBe true
    }
  }

  it should "not stage data when the staging is full" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSizeInBytes", "1")) { (spark, tmpDir) =>
    {
      val df = createDF(spark)
      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "2000")
        .save(tmpDir)

      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "2000")
        .save(tmpDir)

      val snapshot = getQbeastSnapshot(spark, tmpDir)
      val revisions = snapshot.loadAllRevisions
      revisions.size shouldBe 2

      val stagingDataManager = new StagingDataManager(QTableID(tmpDir))

      val indexedDataSize = snapshot
        .loadIndexStatus(1)
        .cubesStatuses
        .values
        .flatMap(_.files.map(_.elementCount))
        .sum

      stagingDataManager invokePrivate getCurrentStagingSize() shouldBe 0L
      indexedDataSize shouldBe 20000L
    }
  }

  it should "clear the staging area by setting spark.qbeast.index.stagingSize=0" in
    withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.qbeast.index.stagingSizeInBytes", "0")) { (spark, tmpDir) =>
      // Write with delta
      val df = createDF(spark)
      df.write
        .mode("overwrite")
        .format("delta")
        .save(tmpDir)

      // Convert delta files into qbeast staging data
      ConvertToQbeastCommand(s"delta.`$tmpDir`", Seq("a", "c"), 5000).run(spark)

      // Clear the staging area having spark.qbeast.index.stagingSize=0
      import spark.implicits._
      Seq(T2(1, 1))
        .toDF()
        .write
        .mode("append")
        .format("qbeast")
        .save(tmpDir)

      val snapshot = getQbeastSnapshot(spark, tmpDir)
      val revisions = snapshot.loadAllRevisions
      revisions.size shouldBe 2

      val stagingDataManager = new StagingDataManager(QTableID(tmpDir))

      val indexedDataSize = snapshot
        .loadIndexStatus(1)
        .cubesStatuses
        .values
        .flatMap(_.files.map(_.elementCount))
        .sum

      stagingDataManager invokePrivate getCurrentStagingSize() shouldBe 0L
      indexedDataSize shouldBe 10001L
    }
}
