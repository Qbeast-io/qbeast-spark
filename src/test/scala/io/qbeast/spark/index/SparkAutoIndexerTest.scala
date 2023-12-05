package io.qbeast.spark.index

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class SparkAutoIndexerTest extends AnyFlatSpec with Matchers {

  "SparkAutoIndexer" should "select correct columns for indexing" in {
    val spark = SparkSession.builder
      .master("local")
      .appName("SparkAutoIndexerTest")
      .getOrCreate()

    import spark.implicits._

    // Sample DataFrame
    val schema = StructType(Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("timestamp", TimestampType, true),
        StructField("value", DoubleType, true)
    ))

    val data = Seq(
        Row(1, "Alice", java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 12.5),
        Row(2, "Bob", java.sql.Timestamp.valueOf("2023-01-02 11:30:00"), 15.0)
        // Add more rows as needed
    )

    val testDF = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
    )

    val autoIndexer = new SparkAutoIndexer()
    val numColumnsToSelect = 2 // Adjust as needed
    val selectedColumns = autoIndexer.chooseColumnsToIndex(testDF, numColumnsToSelect)

    // Assertions
    selectedColumns.length shouldBe numColumnsToSelect
    // Add more assertions as needed

    spark.stop() // Always stop the Spark session after tests
  }

  // Add more tests for other functionalities
}
