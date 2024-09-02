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
package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import io.qbeast.TestUtils._
import org.apache.spark.sql.SparkSession

class QbeastSamplingTest extends QbeastIntegrationTestSpec {

  "Qbeast" should
    "return a valid sample of the original dataset" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)

          writeTestData(data, Seq("user_id", "product_id"), 1000, tmpDir)

          val df = spark.read.format("qbeast").load(tmpDir)
          val dataSize = data.count()
          // We allow a 1% of tolerance in the sampling
          val tolerance = 0.01

          List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(precision => {
            val result = df
              .sample(withReplacement = false, precision)
              .count()
              .toDouble

            result shouldBe (dataSize * precision) +- dataSize * precision * tolerance
          })

          // Testing collect() method
          df.sample(withReplacement = false, 0.1)
            .collect()
            .length
            .toDouble shouldBe (dataSize * 0.1) +- dataSize * 0.1 * tolerance

          data.columns.toSet shouldBe df.columns.toSet

        }
    }

  it should
    "filter the files to read" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 1000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val precision = 0.01

        val query = df.sample(withReplacement = false, precision)
        checkFileFiltering(query)
      }
    }

  def optimize(spark: SparkSession, tmpDir: String, times: Int): Unit = {
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    (0 until times).foreach(_ => {
      qbeastTable.analyze(); qbeastTable.optimize()
    })

  }

  "An optimized index" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 1000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 1)
        val dataSize = data.count()

        df.count() shouldBe dataSize

        val tolerance = 0.01
        List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(precision => {
          val result = df
            .sample(false, precision)
            .count()
            .toDouble

          result shouldBe (dataSize * precision) +- dataSize * precision * tolerance
        })
      }
  }

  "An appended dataset" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        val columnsToIndex = Seq("user_id", "product_id")
        val cubeSize = 10000
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val appendData = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("src/test/resources/ecommerce300k_2019_Nov.csv")

        appendData.write
          .mode("append")
          .format("qbeast")
          .options(
            Map(
              "columnsToIndex" -> columnsToIndex.mkString(","),
              "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val dataSize = data.count() + appendData.count()

        val precision = 0.1
        val tolerance = 0.01
        // We allow a 1% of tolerance in the sampling
        df.sample(withReplacement = false, precision)
          .count()
          .toDouble shouldBe (dataSize * precision) +- dataSize * precision * tolerance

      }
  }

  it should "sample with different column types in SQL" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      {
        val data = loadTestData(spark)
        val cubeSize = 10000
        data.write
          .mode("append")
          .format("qbeast")
          .options(
            Map("columnsToIndex" -> "user_id,brand:histogram", "cubeSize" -> cubeSize.toString))
          .saveAsTable("table")

        val dataSize = data.count()

        val precision = 0.1
        val precisionPercent = precision * 100
        val tolerance = 0.01
        // We allow a 1% of tolerance in the sampling
        spark
          .sql(s"SELECT * FROM table TABLESAMPLE($precisionPercent PERCENT)")
          .count()
          .toDouble shouldBe (dataSize * precision) +- dataSize * precision * tolerance

      }
  }

}
