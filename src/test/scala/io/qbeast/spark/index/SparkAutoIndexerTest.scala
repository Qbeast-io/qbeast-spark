package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec

class SparkAutoIndexerTest extends QbeastIntegrationTestSpec {

  behavior of "SparkAutoIndexer"

  it should "select correct columns for indexing" in withSpark(spark => {

    import spark.implicits._
    // Create test data
    val testDF = Seq(
      (1, "Alice", java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 12.5),
      (2, "Bob", java.sql.Timestamp.valueOf("2023-01-02 11:30:00"), 15.0)
      // Add more rows as needed
    ).toDF("id", "name", "timestamp", "value")

    // Initialize SparkAutoIndexer
    val autoIndexer = SparkAutoIndexer
    val numColumnsToSelect = 2 // Adjust as needed

    // Invoke method
    val selectedColumns = autoIndexer.chooseColumnsToIndex(testDF, numColumnsToSelect)

    // Assertions
    selectedColumns.length shouldBe numColumnsToSelect
    // Additional assertions as needed
    selectedColumns shouldBe Seq("name", "value")
  })

}
