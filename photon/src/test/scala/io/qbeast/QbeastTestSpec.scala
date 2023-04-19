package io.qbeast.spark.sql.connector.catalog

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.qbeast.core.keeper.{Keeper, LocalKeeper}
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

trait QbeastTestSpec extends AnyFlatSpec with Matchers with DatasetComparer {

  val qbeastDataPath = "photon/src/test/resources/ecommerce_qbeast"
  val rawDataPath = "photon/src/test/resources/ecommerce100K_2019_Oct.csv"

  // Spark Configuration
  def sparkConfWithSqlAndCatalog: SparkConf = new SparkConf()
    .setMaster("local[8]")
    .set("spark.sql.extensions", "io.qbeast.spark.sql.QbeastPhotonSessionExtension")

  def loadRawData(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(rawDataPath)

  /**
   * This function is used to create a spark session with the given configuration.
   *
   * @param sparkConf the configuration
   * @param testCode  the code to run within the spark session
   * @tparam T
   * @return
   */
  def withExtendedSpark[T](sparkConf: SparkConf = new SparkConf())(
      testCode: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .appName("QbeastDataSource")
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.WARN.toString)
    try {
      testCode(spark)
    } finally {
      spark.close()
    }
  }

  def withExtendedSparkAndTmpDir[T](sparkConf: SparkConf = new SparkConf())(
      testCode: (SparkSession, String) => T): T = {
    withTmpDir(tmpDir => withExtendedSpark(sparkConf)(spark => testCode(spark, tmpDir)))
  }

  /**
   * This function is used to create a spark session
   *
   * @param testCode the code to test within the spark session
   * @tparam T
   * @return
   */
  def withSpark[T](testCode: SparkSession => T): T = {
    withExtendedSpark(sparkConfWithSqlAndCatalog)(testCode)
  }

  /**
   * Runs code with a Temporary Directory. After execution, the content of the directory is deleted.
   *
   * @param testCode
   * @tparam T
   * @return
   */
  def withTmpDir[T](testCode: String => T): T = {
    val directory = Files.createTempDirectory("qb-testing")
    try {
      testCode(directory.toString)
    } finally {
      import scala.reflect.io.Directory
      val d = new Directory(directory.toFile)
      d.deleteRecursively()
    }
  }

  /**
   * Runs a given test code with a QbeastContext instance. The specified Keeper
   * instance is used to customize the QbeastContext.
   *
   * @param keeper   the keeper
   * @param testCode the test code
   * @tparam T the test result type
   * @return the test result
   */
  def withSparkContext[T](keeper: Keeper = LocalKeeper)(testCode: SparkSession => T): T = {
    withSpark { spark =>
      try {
        testCode(spark)
      } finally {}
    }
  }

  /**
   * Runs code with QbeastContext and a Temporary Directory.
   *
   * @param testCode
   * @tparam T
   * @return
   */

  def withSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir => withSparkContext()(spark => testCode(spark, tmpDir)))

}
