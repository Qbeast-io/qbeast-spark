package io.qbeast.spark.utils

import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * Integration test to check the correctness of the transactions identified by user.
 */
class QbeastSparkTxnTest extends QbeastIntegrationTestSpec {

  "QbeastSpark" should "save SetTransaction action in the log while indexing data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog) { (spark, tmpDir) =>
    val data = makeDataFrame(spark)
    data.write
      .format("qbeast")
      .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
      .option(QbeastOptions.CUBE_SIZE, 1)
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "1")
      .save(tmpDir)
    val transaction =
      DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.setTransactions.head
    transaction.appId shouldBe "test"
    transaction.version shouldBe 1
  }

  it should "save SetTransaction action in the log while staging data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.stagingSizeInBytes", "1000000")) {
    (spark, tmpDir) =>
      val data = makeDataFrame(spark)
      data.write
        .format("qbeast")
        .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
        .option(QbeastOptions.CUBE_SIZE, 1)
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "1")
        .save(tmpDir)
      val transaction =
        DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.setTransactions.head
      transaction.appId shouldBe "test"
      transaction.version shouldBe 1
  }

  it should "ignore aleady processed transaction while indexing data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog) { (spark, tmpDir) =>
    val data = makeDataFrame(spark)
    data.write
      .format("qbeast")
      .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
      .option(QbeastOptions.CUBE_SIZE, 1)
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "1")
      .save(tmpDir)
    data.write
      .format("qbeast")
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "1")
      .mode(SaveMode.Append)
      .save(tmpDir)
    spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count()
  }

  it should "ignore already processed transaction while staging data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.stagingSizeInBytes", "1000000")) {
    (spark, tmpDir) =>
      val data = makeDataFrame(spark)
      data.write
        .format("qbeast")
        .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
        .option(QbeastOptions.CUBE_SIZE, 1)
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "1")
        .save(tmpDir)
      data.write
        .format("qbeast")
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "1")
        .mode(SaveMode.Append)
        .save(tmpDir)
      spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count()
  }

  it should "process transaction with different appId while indexing data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog) { (spark, tmpDir) =>
    val data = makeDataFrame(spark)
    data.write
      .format("qbeast")
      .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
      .option(QbeastOptions.CUBE_SIZE, 1)
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "1")
      .save(tmpDir)
    data.write
      .format("qbeast")
      .option(QbeastOptions.TXN_APP_ID, "test2")
      .option(QbeastOptions.TXN_VERSION, "1")
      .mode(SaveMode.Append)
      .save(tmpDir)
    spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count() * 2
  }

  it should "process transaction with different version while indexing data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog) { (spark, tmpDir) =>
    val data = makeDataFrame(spark)
    data.write
      .format("qbeast")
      .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
      .option(QbeastOptions.CUBE_SIZE, 1)
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "1")
      .save(tmpDir)
    data.write
      .format("qbeast")
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "2")
      .mode(SaveMode.Append)
      .save(tmpDir)
    spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count() * 2
  }

  it should "process transaction with different appId while staging data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.stagingSizeInBytes", "1000000")) {
    (spark, tmpDir) =>
      val data = makeDataFrame(spark)
      data.write
        .format("qbeast")
        .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
        .option(QbeastOptions.CUBE_SIZE, 1)
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "1")
        .save(tmpDir)
      data.write
        .format("qbeast")
        .option(QbeastOptions.TXN_APP_ID, "test2")
        .option(QbeastOptions.TXN_VERSION, "1")
        .mode(SaveMode.Append)
        .save(tmpDir)
      spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count() * 2
  }

  it should "process transaction with different version while staging data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.stagingSizeInBytes", "1000000")) {
    (spark, tmpDir) =>
      val data = makeDataFrame(spark)
      data.write
        .format("qbeast")
        .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
        .option(QbeastOptions.CUBE_SIZE, 1)
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "1")
        .save(tmpDir)
      data.write
        .format("qbeast")
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "2")
        .mode(SaveMode.Append)
        .save(tmpDir)
      spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count() * 2
  }

  it should "process implicit transaction while indexing data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog) { (spark, tmpDir) =>
    val data = makeDataFrame(spark)
    data.write
      .format("qbeast")
      .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
      .option(QbeastOptions.CUBE_SIZE, 1)
      .option(QbeastOptions.TXN_APP_ID, "test")
      .option(QbeastOptions.TXN_VERSION, "1")
      .save(tmpDir)
    data.write
      .format("qbeast")
      .mode(SaveMode.Append)
      .save(tmpDir)
    spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count() * 2
  }

  it should "process implicit transaction while staging data" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.stagingSizeInBytes", "1000000")) {
    (spark, tmpDir) =>
      val data = makeDataFrame(spark)
      data.write
        .format("qbeast")
        .option(QbeastOptions.COLUMNS_TO_INDEX, "id")
        .option(QbeastOptions.CUBE_SIZE, 1)
        .option(QbeastOptions.TXN_APP_ID, "test")
        .option(QbeastOptions.TXN_VERSION, "1")
        .save(tmpDir)
      data.write
        .format("qbeast")
        .mode(SaveMode.Append)
        .save(tmpDir)
      spark.read.format("qbeast").load(tmpDir).count() shouldBe data.count() * 2
  }

  private def makeDataFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._
    (1 to 3).map(i => Student(i, i.toString(), Random.nextInt)).toDF()
  }

}
