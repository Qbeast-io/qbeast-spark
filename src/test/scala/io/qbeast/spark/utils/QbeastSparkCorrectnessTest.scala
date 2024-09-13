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
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.AnalysisException

class QbeastSparkCorrectnessTest extends QbeastIntegrationTestSpec {

  "Qbeast datasource" should
    "expose the original number of columns and rows" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("qbeast").load(tmpDir)

          data.count() shouldBe indexed.count()

          assertLargeDatasetEquality(indexed, data, orderedComparison = false)

          data.columns.toSet shouldBe indexed.columns.toSet

        }
    }

  "Qbeast index" should "index correctly on bigger spaces" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
          .withColumn("user_id", lit(col("user_id") * Long.MaxValue))
        // WRITE SOME DATA
        data.write
          .mode("overwrite")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        data.count() shouldBe indexed.count()

        assertLargeDatasetEquality(indexed, data, orderedComparison = false)

        data.columns.toSet shouldBe indexed.columns.toSet

      }
  }

  it should "index correctly on overwrite" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val data = loadTestData(spark)
      // WRITE SOME DATA
      writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

      // OVERWRITE
      writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)

      data.count() shouldBe indexed.count()

      assertLargeDatasetEquality(indexed, data, orderedComparison = false)

      data.columns.toSet shouldBe indexed.columns.toSet
    }
  }

  it should "clean previous metadata on overwrite" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)
        // WRITE SOME DATA
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        // OVERWRITE
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)

        // Include the staging revision
        qbeastSnapshot.loadAllRevisions.size shouldBe 2
        qbeastSnapshot.loadLatestRevision.revisionID shouldBe 1L

      }
  }

  it should
    "work with indexed columns within 0 and 1" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          import org.apache.spark.sql.functions._
          import spark.implicits._
          val data = loadTestData(spark)
          val stats = data
            .agg(
              max('user_id).as("max_user_id"),
              min('user_id).as("min_user_id"),
              max('product_id).as("max_product_id"),
              min('product_id).as("min_product_id"))
            .collect()
            .head
          val (max_user, min_user, max_p, min_p) = {
            (stats.getInt(0), stats.getInt(1), stats.getInt(2), stats.getInt(3))
          }
          val norm_user = udf((v: Int) => (v - min_user).toDouble / (max_user - min_user))
          val norm_p = udf((v: Int) => (v - min_p).toDouble / (max_p - min_p))

          val normalizedData = data
            .withColumn("tmp_user_id", norm_user('user_id))
            .withColumn("tmp_norm_p", norm_p('product_id))
            .drop("user_id", "product_id")
            .withColumnRenamed("tmp_user_id", "user_id")
            .withColumnRenamed("tmp_norm_p", "product_id")

          writeTestData(normalizedData, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("qbeast").load(tmpDir)

          normalizedData.count() shouldBe indexed.count()

          assertLargeDatasetEquality(indexed, normalizedData, orderedComparison = false)

          normalizedData.columns.toSet shouldBe indexed.columns.toSet

        }
    }

  it should
    "append data to the original dataset" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
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

        df.count() shouldBe dataSize

      }
    }

  it should "work without specifying columnsToIndex " +
    "while cause revision change by using a different cubeSize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
        original.write
          .format("qbeast")
          .option("cubeSize", 10000)
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        original.write
          .mode("append")
          .format("qbeast")
          .option("cubeSize", 5000)
          .save(tmpDir)
        val qDf = spark.read.format("qbeast").load(tmpDir)

        qDf.count shouldBe original.count * 2
      }
    }

  it should "append to an existing qbeast table without specifying cubeSize" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
        original.write
          .format("qbeast")
          .option("cubeSize", 10000)
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        original.write
          .mode("append")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)
        val qDf = spark.read.format("qbeast").load(tmpDir)

        qDf.count shouldBe original.count * 2
      }
    }

  "Appending to an non-existing table" should
    "throw an exception if 'columnsToIndex' is not provided" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val original = loadTestData(spark)
          a[AnalysisException] shouldBe thrownBy {
            original.write
              .format("qbeast")
              .option("cubeSize", 10000)
              .save(tmpDir)
          }
        }
    }

  "Appends" should "only update metadata when needed" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = loadTestData(spark).limit(5000)
        df.write
          .format("qbeast")
          .option("columnsToIndex", "user_id,price")
          .save(tmpDir)
        df.write.mode("append").format("qbeast").save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val conf = deltaLog.newDeltaHadoopConf()
        deltaLog.store
          .read(FileNames.deltaFile(deltaLog.logPath, 0L), conf)
          .map(Action.fromJson)
          .collect { case a: Metadata => a }
          .isEmpty shouldBe false

        deltaLog.store
          .read(FileNames.deltaFile(deltaLog.logPath, 1L), conf)
          .map(Action.fromJson)
          .collect { case a: Metadata => a }
          .isEmpty shouldBe true
      }
    }

}
