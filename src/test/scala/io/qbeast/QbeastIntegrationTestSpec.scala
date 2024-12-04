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
package io.qbeast

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import io.qbeast.context.QbeastContext
import io.qbeast.context.QbeastContextImpl
import io.qbeast.core.model.IndexManager
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastSnapshot
import io.qbeast.spark.index.SparkColumnsToIndexSelector
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.table.IndexedTableFactoryImpl
import org.apache.log4j.Level
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import scala.reflect.io.Directory

/**
 * This class contains all function that you should use to test qbeast over spark. You can use it
 * like:
 * {{{
 *  "my class" should "write correctly the data" in withSparkAndTmpDir {
 *   (spark,tmpDir) =>{
 *       List(("hola",1)).toDF.write.parquet(tmpDir)
 *
 *  }
 * }}}
 */
trait QbeastIntegrationTestSpec extends AnyFlatSpec with Matchers with DatasetComparer {

  // Spark Configuration
  // Including Session Extensions and Catalog
  def sparkConfWithSqlAndCatalog: SparkConf = new SparkConf()
    .setMaster("local[8]")
    .set("spark.sql.extensions", "io.qbeast.sql.QbeastSparkSessionExtension")
    .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, "io.qbeast.catalog.QbeastCatalog")

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
   * @param sparkConf
   *   the configuration
   * @param testCode
   *   the code to run within the spark session
   * @tparam T
   *   the type of the output
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
   * @param testCode
   *   the code to test within the spark session
   * @tparam T
   *   the type of the output
   * @return
   */
  def withSpark[T](testCode: SparkSession => T): T = {
    withExtendedSpark(sparkConfWithSqlAndCatalog)(testCode)
  }

  /**
   * Runs code with a Temporary Directory. After execution, the content of the directory is
   * deleted.
   * @param testCode
   *   the test code
   * @tparam T
   *   the test result type
   * @return
   */
  def withTmpDir[T](testCode: String => T): T = {
    val directory = Files.createTempDirectory("qb-testing")
    try {
      testCode(directory.toString)
    } finally {
      removeDirectory(directory.toString)
    }
  }

  def removeDirectory(directoryPath: String): Unit = {
    val directory = new File(directoryPath)
    if (directory.exists() && directory.isDirectory) {
      val d = new Directory(directory)
      d.deleteRecursively()
    }
  }

  def withSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir => withSpark(spark => testCode(spark, tmpDir)))

  /**
   * Runs a given test code with a QbeastContext instance.
   * @param testCode
   *   the test code
   * @tparam T
   *   the test result type
   * @return
   *   the test result
   */
  def withQbeastAndSparkContext[T]()(testCode: SparkSession => T): T = {
    withSpark { spark =>
      val indexedTableFactory = new IndexedTableFactoryImpl(
        QbeastContext.indexManager,
        QbeastContext.metadataManager,
        QbeastContext.dataWriter,
        SparkRevisionFactory,
        SparkColumnsToIndexSelector)
      val context = new QbeastContextImpl(spark.sparkContext.getConf, indexedTableFactory)
      try {
        QbeastContext.setUnmanaged(context)
        testCode(spark)
      } finally {
        QbeastContext.unsetUnmanaged()
      }
    }
  }

  /**
   * Runs code with QbeastContext and a Temporary Directory.
   * @param testCode
   *   actual test code
   * @tparam T
   *   output type
   * @return
   */

  def withQbeastContextSparkAndTmpDir[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir => withQbeastAndSparkContext()(spark => testCode(spark, tmpDir)))

  /**
   * Runs code with Warehouse/Catalog extensions
   * @param testCode
   *   the code to reproduce
   * @tparam T
   *   the type of the output
   * @return
   */
  def withQbeastContextSparkAndTmpWarehouse[T](testCode: (SparkSession, String) => T): T =
    withTmpDir(tmpDir =>
      withExtendedSpark(
        sparkConfWithSqlAndCatalog
          .set("spark.sql.warehouse.dir", tmpDir))(spark => testCode(spark, tmpDir)))

  /**
   * Runs code with OTreeAlgorithm configuration
   * @param code
   *   IndexManager
   * @tparam T
   *   code type
   * @return
   */
  def withOTreeAlgorithm[T](code: IndexManager => T): T = {
    code(SparkOTreeManager)
  }

  def getQbeastSnapshot(dir: String): QbeastSnapshot = {
    val tableId = new QTableID(dir)
    QbeastContext.metadataManager.loadSnapshot(tableId)
  }

}
