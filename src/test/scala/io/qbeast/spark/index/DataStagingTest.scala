package io.qbeast.spark.index

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.RevisionUtils.{isStaging, stagingID}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.DeltaLog

class DataStagingTest extends QbeastIntegrationTestSpec {

  def createDF(spark: SparkSession): DataFrame = {
    import spark.implicits._

    (0 until 10000).map(i => T2(i, i)).toDF("a", "b")
  }

  def getQbeastSnapshot(spark: SparkSession, dir: String): DeltaQbeastSnapshot = {
    val deltaLog = DeltaLog.forTable(spark, dir)
    DeltaQbeastSnapshot(deltaLog.snapshot)
  }

  "Data Staging" should "stage data during first write" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSize", "1")) { (spark, tmpDir) =>
    {
      val df = createDF(spark)
      df.write
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .option("cubeSize", "2000")
        .save(tmpDir)

      val revisions = getQbeastSnapshot(spark, tmpDir).loadAllRevisions
      revisions.size shouldBe 1
      isStaging(revisions.head) shouldBe true
    }
  }

  it should "not stage data when the staging is full" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSize", "1")) { (spark, tmpDir) =>
    {
      val df = createDF(spark)
      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .option("cubeSize", "2000")
        .save(tmpDir)

      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .option("cubeSize", "2000")
        .save(tmpDir)

      val snapshot = getQbeastSnapshot(spark, tmpDir)
      val revisions = snapshot.loadAllRevisions
      revisions.size shouldBe 2

      val stagingSize = snapshot
        .loadIndexStatus(stagingID)
        .cubesStatuses
        .values
        .flatMap(_.files.map(_.elementCount))
        .sum

      val indexedDataSize = snapshot
        .loadIndexStatus(1)
        .cubesStatuses
        .values
        .flatMap(_.files.map(_.elementCount))
        .sum

      stagingSize shouldBe 0L
      indexedDataSize shouldBe 20000L
    }
  }

}
