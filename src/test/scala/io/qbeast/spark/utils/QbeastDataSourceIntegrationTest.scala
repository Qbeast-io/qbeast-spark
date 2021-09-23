/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.CubeId
import io.qbeast.spark.sql.files.OTreeIndex
import io.qbeast.spark.table.QbeastTable
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.{DataFrame, SparkSession}

class QbeastDataSourceIntegrationTest extends QbeastIntegrationTestSpec {

  private def loadTestData(spark: SparkSession): DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/test/resources/ecommerce100K_2019_Oct.csv")
    .distinct()

  "the Qbeast data source" should
    "expose the original number of columns and rows" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
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
      data.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "user_id,product_id")
        .save(tmpDir)

      // OVERWRITE
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

          normalizedData.write
            .mode("overwrite")
            .format("qbeast")
            .option("columnsToIndex", "user_id,product_id")
            .save(tmpDir)

          val indexed = spark.read.format("qbeast").load(tmpDir)

          normalizedData.count() shouldBe indexed.count()

          assertLargeDatasetEquality(indexed, normalizedData, orderedComparison = false)

          normalizedData.columns.toSet shouldBe indexed.columns.toSet

        }
    }
  "the Qbeast data source" should
    "not replicate any point if there are optimizations" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
          data.write
            .mode("error")
            .format("qbeast")
            .option("columnsToIndex", "user_id,product_id")
            .save(tmpDir)

          val indexed = spark.read.format("parquet").load(tmpDir)
          assertLargeDatasetEquality(indexed, data, orderedComparison = false)

        }
    }

  it should
    "filter the files to read" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        data.write
          .mode("error")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)
        val df = spark.read.format("qbeast").load(tmpDir)
        val precision = 0.1

        val query = df.sample(withReplacement = false, precision)
        val executionPlan = query.queryExecution.executedPlan.collectLeaves()

        assert(
          executionPlan.exists(p =>
            p
              .asInstanceOf[FileSourceScanExec]
              .relation
              .location
              .isInstanceOf[OTreeIndex]))

        executionPlan
          .foreach {
            case f: FileSourceScanExec if f.relation.location.isInstanceOf[OTreeIndex] =>
              val index = f.relation.location
              val matchingFiles =
                index.listFiles(f.partitionFilters, f.dataFilters).flatMap(_.files)
              val allFiles = index.inputFiles
              matchingFiles.length shouldBe <(allFiles.length)
          }
      }
    }

  it should
    "return a valid sample of the original dataset" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)

          data.write
            .mode("error")
            .format("qbeast")
            .option("columnsToIndex", "user_id,product_id")
            .save(tmpDir)
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
    "append data to the original dataset" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        data.write
          .mode("error")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val appendData = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("src/test/resources/ecommerce300k_2019_Nov.csv")

        appendData.write
          .mode("append")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        val dataSize = data.count() + appendData.count()

        df.count() shouldBe dataSize

        val precision = 0.1
        val tolerance = 0.01
        // We allow a 1% of tolerance in the sampling
        df.sample(withReplacement = false, precision)
          .count()
          .toDouble shouldBe (dataSize * precision) +- dataSize * precision * tolerance

      }
    }

  def optimize(spark: SparkSession, tmpDir: String, times: Int): Unit = {
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    (0 until times).foreach(_ => { qbeastTable.analyze(); qbeastTable.optimize() })

  }

  "An optimized index" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        data.write
          .mode("error")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)
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

  it should "erase cube information when overwrited" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        // val tmpDir = "/tmp/qbeast3"
        val data = loadTestData(spark)

        data.write
          .mode("error")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)

        // Overwrite table
        data.write
          .mode("overwrite")
          .format("qbeast")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)

        qbeastTable.analyze() shouldBe Seq(CubeId.root(2).string)

      }
  }

}
