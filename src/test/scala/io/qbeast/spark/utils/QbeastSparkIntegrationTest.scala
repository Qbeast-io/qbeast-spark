package io.qbeast.spark.utils

import io.qbeast.TestClasses.Student
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

class QbeastSparkIntegrationTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private val schema = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

  private def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val data = students.toDF()
    spark.createDataFrame(data.rdd, schema)
  }

  "The QbeastDataSource" should
    "work with DataFrame API" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createTestData(spark)
        data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        indexed.count() shouldBe data.count()

        indexed.columns.toSet shouldBe data.columns.toSet

        assertSmallDatasetEquality(indexed, data, orderedComparison = false)

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

      val indexed = spark.read.format("qbeast").load(location)

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

      assertSmallDatasetEquality(indexed, data, orderedComparison = false)

    }
  }

  it should "work with InsertInto" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val data = createTestData(spark)
      val location = tmpDir + "/external"
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("location", location)
        .saveAsTable("qbeast")

      val newData = data
      newData.write.insertInto("qbeast")

      val indexed = spark.read.table("qbeast")
      val allData = data.union(data)

      indexed.count() shouldBe allData.count()

      indexed.columns.toSet shouldBe allData.columns.toSet

      assertSmallDatasetEquality(indexed, allData, orderedComparison = false)
    }
  }

}
