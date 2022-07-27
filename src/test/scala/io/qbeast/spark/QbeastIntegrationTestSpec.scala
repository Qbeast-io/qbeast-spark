/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.qbeast.core.keeper.{Keeper, LocalKeeper}
import io.qbeast.context.{QbeastContext, QbeastContextImpl}
import io.qbeast.core.model.IndexManager
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.{SparkOTreeManager, SparkRevisionFactory}
import io.qbeast.spark.index.writer.SparkDataWriter
import io.qbeast.spark.internal.QbeastSparkSessionExtension
import io.qbeast.spark.table.IndexedTableFactoryImpl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * This class contains all function that you should use to test qbeast over spark.
 * You can use it like:
 * {{{
 *  "my class" should "write correctly the data" in withSparkAndTmpDir {
 *   (spark,tmpDir) =>{
 *       List(("hola",1)).toDF.write.parquet(tmpDir)
 *
 *  }
 * }}}
 */
trait QbeastIntegrationTestSpec extends AnyFlatSpec with Matchers with DatasetComparer {
  // This reduce the verbosity of Spark
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def loadTestData(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/test/resources/ecommerce100K_2019_Oct.csv")
    .distinct()

  def writeTestData(
      data: DataFrame,
      columnsToIndex: Seq[String],
      cubeSize: Int,
      tmpDir: String,
      mode: String = "overwrite"): Unit = {

    data.write
      .mode(mode)
      .format("qbeast")
      .options(
        Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString))
      .save(tmpDir)
  }

  /**
   * This function is used to create a spark session with the given configuration.
   * @param sparkConf the configuration
   * @param testCode the code to run within the spark session
   * @tparam T
   * @return
   */
  def withExtendedSpark[T](sparkConf: SparkConf = new SparkConf())(
      testCode: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("QbeastDataSource")
      .withExtensions(new QbeastSparkSessionExtension())
      .config(sparkConf)
      .getOrCreate()
    try {
      testCode(spark)
    } finally {
      spark.close()
    }
  }

  /**
   * This function is used to create a spark session
   * @param testCode the code to test within the spark session
   * @tparam T
   * @return
   */
  def withSpark[T](testCode: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("QbeastDataSource")
      .withExtensions(new QbeastSparkSessionExtension())
      .getOrCreate()
    try {
      testCode(spark)
    } finally {
      spark.close()
    }
  }

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

  def withSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir => withSpark(spark => testCode(spark, tmpDir)))

  /**
   * Runs a given test code with a QbeastContext instance. The specified Keeper
   * instance is used to customize the QbeastContext.
   *
   * @param keeper the keeper
   * @param testCode the test code
   * @tparam T the test result type
   * @return the test result
   */
  def withQbeastAndSparkContext[T](keeper: Keeper = LocalKeeper)(
      testCode: SparkSession => T): T = {
    withSpark { spark =>
      val indexedTableFactory = new IndexedTableFactoryImpl(
        keeper,
        SparkOTreeManager,
        SparkDeltaMetadataManager,
        SparkDataWriter,
        SparkRevisionFactory)
      val context = new QbeastContextImpl(spark.sparkContext.getConf, keeper, indexedTableFactory)
      try {
        QbeastContext.setUnmanaged(context)
        testCode(spark)
      } finally {
        QbeastContext.unsetUnmanaged()
      }
    }
  }

  def withQbeastContextSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir => withQbeastAndSparkContext()(spark => testCode(spark, tmpDir)))

  def withQbeastContextSparkAndTmpWarehouse[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir =>
      withExtendedSpark(new SparkConf().set("spark.sql.warehouse.dir", tmpDir))(spark =>
        testCode(spark, tmpDir)))

  def withOTreeAlgorithm[T](code: IndexManager[DataFrame] => T): T = {
    code(SparkOTreeManager)
  }

}
