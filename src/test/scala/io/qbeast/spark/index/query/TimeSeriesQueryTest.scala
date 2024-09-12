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
package io.qbeast.spark.index.query

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.unix_timestamp

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
