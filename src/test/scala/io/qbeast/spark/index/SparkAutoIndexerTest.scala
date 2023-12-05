package io.qbeast.spark.index

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index._

class SparkAutoIndexerTest extends QbeastIntegrationTestSpec {

  behavior of "SparkAutoIndexer"

  it should "select correct columns for indexing" in withSpark(spark => {

    // Define schema
    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("timestamp", TimestampType, true),
      StructField("value", DoubleType, true)
    ))

    // Create test data
    val data = Seq(
      Row(1, "Alice", java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 12.5),
      Row(2, "Bob", java.sql.Timestamp.valueOf("2023-01-02 11:30:00"), 15.0)
      // Add more rows as needed
    )

    // Create DataFrame
    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Initialize SparkAutoIndexer
    val autoIndexer = SparkAutoIndexer
    val numColumnsToSelect = 2 // Adjust as needed

    // Invoke method
    val selectedColumns = autoIndexer.chooseColumnsToIndex(testDF, numColumnsToSelect)

    // Assertions
    selectedColumns.length shouldBe numColumnsToSelect
    // Additional assertions as needed
  })

}
