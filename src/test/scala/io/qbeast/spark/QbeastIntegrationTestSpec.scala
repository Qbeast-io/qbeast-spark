/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.typesafe.config.{Config, ConfigFactory}
import io.qbeast.core.keeper.{Keeper, LocalKeeper}
import io.qbeast.context.{QbeastContext, QbeastContextImpl}
import io.qbeast.core.model.IndexManager
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.{SparkOTreeManager, SparkRevisionFactory}
import io.qbeast.spark.index.writer.SparkDataWriter
import io.qbeast.spark.internal.QbeastSparkSessionExtension
import io.qbeast.spark.table.IndexedTableFactoryImpl
import org.apache.log4j.{Level, Logger}
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

  def withExtendedSpark[T](testCode: SparkSession => T): T = {
    val spark = SparkSession
      .builder()
      .master("local[8]")
      .appName("QbeastDataSource")
      .withExtensions(new QbeastSparkSessionExtension())
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
      .getOrCreate()
    try {
      testCode(spark)
    } finally {
      spark.close()
    }
  }

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
  def withQbeastContext[T](keeper: Keeper = LocalKeeper, config: Config = ConfigFactory.load())(
      testCode: => T): T = {
    val indexedTableFactory = new IndexedTableFactoryImpl(
      keeper,
      SparkOTreeManager,
      SparkDeltaMetadataManager,
      SparkDataWriter,
      SparkRevisionFactory)
    val context = new QbeastContextImpl(config, keeper, indexedTableFactory)
    try {
      QbeastContext.setUnmanaged(context)
      testCode
    } finally {
      QbeastContext.unsetUnmanaged()
    }
  }

  def withQbeastContextSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withQbeastContext()(withTmpDir(tmpDir => withSpark(spark => testCode(spark, tmpDir))))

  def withOTreeAlgorithm[T](code: IndexManager[DataFrame] => T): T = {
    code(SparkOTreeManager)
  }

}
