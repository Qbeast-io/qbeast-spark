package io.qbeast.spark.index.query

import io.qbeast.spark.{QbeastIntegrationTestSpec}
import org.apache.spark.sql.functions.{to_date, to_timestamp, unix_timestamp}

class TimeSeriesQueryTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  "Qbeast Format" should "filter correctly values with Timestamp" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq(
          "2017-01-03 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00")
          .toDF("date")
          .withColumn("date", to_timestamp($"date"))

      df.write.format("qbeast").option("columnsToIndex", "date").save(tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)

      indexed.filter("date == '2017-01-02 12:02:00'").count() shouldBe 3

      indexed.filter("date > '2017-01-02 12:02:00'").count() shouldBe 1

      indexed.filter("date < '2017-01-02 12:02:00'").count() shouldBe 2

    })

  it should "filter correctly values with Date" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df =
      Seq("2017-01-01", "2017-01-02", "2017-01-03", "2017-01-04")
        .toDF("date")
        .withColumn("date", to_date($"date"))

    df.write.format("qbeast").option("columnsToIndex", "date").save(tmpDir)

    val indexed = spark.read.format("qbeast").load(tmpDir)

    indexed.filter("date == '2017-01-03'").count() shouldBe 1

  })

  it should "filter correctly values with complex Dates" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq(
          "2017-01-03 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00")
          .toDF("date")
          .withColumn("date", to_date($"date"))

      df.write.format("qbeast").option("columnsToIndex", "date").save(tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)

      indexed.filter("date == '2017-01-02 12:02:00'").count() shouldBe 3

    })

  it should "filter correctly values with Unix Timestamp" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq(
          "2017-01-03 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-02 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00")
          .toDF("date")
          .withColumn("date", unix_timestamp($"date"))

      df.write.format("qbeast").option("columnsToIndex", "date").save(tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)
      val valueToFilter = df.first().getLong(0)

      indexed.filter(s"date == $valueToFilter").count() shouldBe df
        .filter(s"date == $valueToFilter")
        .count()

    })

}
