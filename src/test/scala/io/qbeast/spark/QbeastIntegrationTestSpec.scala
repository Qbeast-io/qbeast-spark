/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.typesafe.config.{Config, ConfigFactory}
import io.qbeast.spark.context.{QbeastContext, QbeastContextImpl}
import io.qbeast.spark.index.{OTreeAlgorithm, OTreeAlgorithmImpl}
import io.qbeast.spark.keeper.{Keeper, LocalKeeper}
import io.qbeast.spark.sql.QbeastSparkSessionExtension
import io.qbeast.spark.sql.files.OTreeIndex
import io.qbeast.spark.table.IndexedTableFactoryImpl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.{FileSourceScanExec}
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

  def checkFileFiltering(query: DataFrame): Unit = {

    val leaves = query.queryExecution.executedPlan.collectLeaves()

    assert(
      leaves.exists(p =>
        p
          .asInstanceOf[FileSourceScanExec]
          .relation
          .location
          .isInstanceOf[OTreeIndex]))

    leaves
      .foreach {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[OTreeIndex] =>
          val index = f.relation.location
          val matchingFiles =
            index.listFiles(f.partitionFilters, f.dataFilters).flatMap(_.files)
          val allFiles = index.inputFiles
          matchingFiles.length shouldBe <(allFiles.length)
      }

  }

  def withSpark[T](testCode: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
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
  def withQbeastContext[T](keeper: Keeper = LocalKeeper, config: Config = ConfigFactory.load())(
      testCode: => T): T = {
    val desiredSampleSize = config.getInt("qbeast.index.size")
    val oTreeAlgorithm = new OTreeAlgorithmImpl(desiredSampleSize)
    val indexedTableFactory = new IndexedTableFactoryImpl(keeper, oTreeAlgorithm)
    val context = new QbeastContextImpl(config, keeper, oTreeAlgorithm, indexedTableFactory)
    try {
      QbeastContext.setUnmanaged(context)
      testCode
    } finally {
      QbeastContext.unsetUnmanaged()
    }
  }

  def withQbeastContextSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withQbeastContext()(withTmpDir(tmpDir => withSpark(spark => testCode(spark, tmpDir))))

  def withOTreeAlgorithm[T](code: OTreeAlgorithm => T): T = {
    val config = ConfigFactory.load()
    val oTreeAlgorithm = new OTreeAlgorithmImpl(
      desiredCubeSize = config.getInt("qbeast.index.size"))
    code(oTreeAlgorithm)
  }

}
