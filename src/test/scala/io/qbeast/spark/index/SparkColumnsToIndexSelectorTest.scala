package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec

class SparkColumnsToIndexSelectorTest extends QbeastIntegrationTestSpec {

  behavior of "SparkColumnsToIndexSelector"

  it should "select correct columns for indexing" in withSpark(spark => {

    import spark.implicits._
    // Create test data
    val testDF = Seq(
      (1, "Alice", java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 12.5),
      (2, "Bob", java.sql.Timestamp.valueOf("2023-01-02 11:30:00"), 15.0)
      // Add more rows as needed
    ).toDF("id", "name", "timestamp", "value")

    // Initialize SparkColumnsToIndexSelector
    val autoIndexer = SparkColumnsToIndexSelector
    val numColumnsToSelect = 2 // Adjust as needed
    val selectedColumns = autoIndexer.selectColumnsToIndex(testDF, numColumnsToSelect)

    // Assertions
    selectedColumns.length shouldBe numColumnsToSelect
    selectedColumns should contain theSameElementsAs Seq("name", "value")
  })

  it should "not discard string columns" in withSpark(spark => {

    import spark.implicits._
    // Create test data
    val testDF = Seq(
      ("a", 20),
      ("b", 30),
      ("c", 40)
      // Add more rows as needed
    ).toDF("s", "i")

    // Initialize SparkColumnsToIndexSelector
    val autoIndexer = SparkColumnsToIndexSelector
    val selectedColumns = autoIndexer.selectColumnsToIndex(testDF)

    selectedColumns should contain theSameElementsAs testDF.columns
  })

  // TODO - Check if this should be the default behavior
  it should "select maximum 3 columns by default" in withSpark(spark => {

    import spark.implicits._
    // Create test data
    val testDF = Seq(
      (1, "Alice", java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 12.5),
      (2, "Bob", java.sql.Timestamp.valueOf("2023-01-02 11:30:00"), 15.0)
      // Add more rows as needed
    ).toDF("id", "name", "timestamp", "value")

    // Initialize SparkColumnsToIndexSelector
    val autoIndexer = SparkColumnsToIndexSelector
    val selectedColumns = autoIndexer.selectColumnsToIndex(testDF)

    selectedColumns.length shouldBe 3
  })

  it should "select all columns if maxColumnsToIndex > num columns of dataframe" in withExtendedSpark(
    sparkConf =
      sparkConfWithSqlAndCatalog.set("spark.qbeast.index.columnsToIndex.auto.max", "10"))(
    spark => {

      import spark.implicits._
      // Create test data
      val testDF = Seq(
        (1, 5, java.sql.Timestamp.valueOf("2023-01-01 10:00:00"), 12.5),
        (2, 6, java.sql.Timestamp.valueOf("2023-01-02 11:30:00"), 15.0)
        // Add more rows as needed
      ).toDF("id", "name", "timestamp", "value")

      // Initialize SparkColumnsToIndexSelector
      val autoIndexer = SparkColumnsToIndexSelector

      // Invoke method
      val selectedColumns = autoIndexer.selectColumnsToIndex(testDF)

      // Assertions
      selectedColumns.length shouldBe 4 // 4 columns in the test data
      selectedColumns should contain theSameElementsAs testDF.columns
    })

  it should "not select more than maxColumnsToIndex" in withExtendedSpark(sparkConf =
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.columnsToIndex.auto.max", "10"))(spark => {

    import spark.implicits._
    val largeColumnDF =
      Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
        .toDF()

    val autoIndexer = SparkColumnsToIndexSelector
    val selectedLargeColumns = autoIndexer.selectColumnsToIndex(largeColumnDF)
    selectedLargeColumns.length shouldBe 10
  })

  // TODO - Check if this should be the default behavior
  it should "use the 3 first columns if no data is provided" in withSpark(spark => {

    import spark.implicits._
    // Create test data
    val testDF = Seq
      .empty[(Int, String, java.sql.Timestamp, Double)]
      .toDF("id", "name", "timestamp", "value")

    // Initialize SparkColumnsToIndexSelector
    val autoIndexer = SparkColumnsToIndexSelector
    val selectedColumns = autoIndexer.selectColumnsToIndex(testDF)

    // Assertions
    selectedColumns should contain theSameElementsAs testDF.columns.take(3)
  })

}
