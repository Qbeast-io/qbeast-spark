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

  it should "work with InsertInto" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      // TODO
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      // Create the table
      val indexedTable = spark.read.format("qbeast").load(tmpDir)
      indexedTable.createOrReplaceTempView("qbeast")

      // Insert Into table
      data.write.format("qbeast").insertInto("default.qbeast")

      val indexed = spark.read.format("qbeast").load(tmpDir)

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet
    }
  }

  it should "work with SaveAsTable" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val data = createTestData(spark)
      val location = tmpDir + "/external"
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("location", location)
        .saveAsTable("qbeast")

      // spark.catalog.listTables().show(false)

      // val indexed = spark.table("qbeast")
      val indexed = spark.read.format("qbeast").load(location)

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

    }
  }

}
