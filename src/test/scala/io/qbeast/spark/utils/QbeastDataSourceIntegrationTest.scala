/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.{DataFrame, SparkSession}

class QbeastDataSourceIntegrationTest extends QbeastIntegrationTestSpec {

  def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(1, 2, 3, 4).toDF("id")
  }

  "The QbeastDataSource" should
    "work with DataFrame API" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createTestData(spark)
        data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        indexed.count() shouldBe 4

        indexed.columns shouldBe Seq("id")

        indexed.orderBy("id").collect() shouldBe data.orderBy("id").collect()

      }
    }

  it should "work with SaveAsTable" in withQbeastContextSparkAndTmpWarehouse {
    (spark, tmpDirWarehouse) =>
      {
        // TODO
        // DataFrame API
        val data = createTestData(spark)
        data.write.format("qbeast").saveAsTable("default.qbeast")

        val indexed = spark.read.load("default.qbeast")

        indexed.count() shouldBe data.count()

        indexed.columns.toSet shouldBe data.columns.toSet

      }
  }

}
