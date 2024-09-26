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
package io.qbeast.spark.docs

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.SparkConf
import org.scalatest.AppendedClues.convertToClueful

class DocumentationTests extends QbeastIntegrationTestSpec {

  val config: SparkConf = sparkConfWithSqlAndCatalog.set(
    "spark.hadoop.fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

  behavior of "Documentation"

  it should "behave correctly Readme" in withSpark { spark =>
    withTmpDir { tmp_dir =>
      val csv_df = spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("./src/test/resources/ecommerce100K_2019_Oct.csv")

      csv_df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "user_id,product_id")
        .option("cubeSize", 10000)
        .save(tmp_dir)

      val qbeast_df =
        spark.read
          .format("qbeast")
          .load(tmp_dir)

      qbeast_df.count() shouldBe csv_df
        .count() withClue "Readme count does not match the original"

      qbeast_df.schema shouldBe csv_df.schema withClue "Readme schema does not match the original"

    }
  }

  it should "behave correctly in Quickstart DF API" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      import spark.implicits._

      // CREATE A TABLE
      val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "age")
      data.write.mode("overwrite").option("columnsToIndex", "id,age").saveAsTable("qbeast_table")
      val qbeast_df = spark.read.table("qbeast_table")

      assertSmallDatasetEquality(
        qbeast_df,
        data,
        orderedComparison = false,
        ignoreNullable = true)

      // APPEND TO A TABLE
      val append = Seq((4, "d"), (5, "e")).toDF("id", "age")
      append.write.mode("append").insertInto("qbeast_table")
      val qbeast_df_appended = spark.read.table("qbeast_table")

      assertSmallDatasetEquality(
        qbeast_df_appended,
        data.union(append),
        orderedComparison = false,
        ignoreNullable = true)

      // SAMPLE THE TABLE
      val query = qbeast_df.sample(0.5)
      query.count() shouldBe >=(1L)

      // APPEND TO A PATH
      val path = s"$tmpWarehouse/tmp/qbeast_table"
      append.write
        .mode("append")
        .option("columnsToIndex", "id,age")
        .format("qbeast")
        .save(path)

      val qbeast_df_appended_path = spark.read.format("qbeast").load(path)
      assertSmallDatasetEquality(
        qbeast_df_appended_path,
        append,
        orderedComparison = false,
        ignoreNullable = true)
    })

  it should "behave correctly in Quickstart SQL API for INSERT INTO" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {
      import spark.implicits._

      // CREATE A TABLE
      spark.sql(
        "CREATE TABLE qbeast_table (id INT, age STRING) USING qbeast OPTIONS (columnsToIndex 'id,age')")

      // INSERT INTO A TABLE
      spark.sql("INSERT INTO qbeast_table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      val qbeast_df = spark.read.table("qbeast_table")
      val data = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "age")

      assertSmallDatasetEquality(
        qbeast_df,
        data,
        orderedComparison = false,
        ignoreNullable = true)

      // SAMPLE THE TABLE
      val query = spark.sql("SELECT * FROM qbeast_table TABLESAMPLE(30 PERCENT)")
      query.count() shouldBe >=(1L)

    })

  it should "behave correctly on Sample Pushdown Notebook" in withExtendedSpark(config) { spark =>
    withTmpDir { DATA_ROOT =>
      val parquet_table_path = "s3a://qbeast-public-datasets/store_sales"
      val qbeast_table_path = DATA_ROOT + "/qbeast/qtable"

      val parquet_df = spark.read.format("parquet").load(parquet_table_path)

      val processed_parquet_df =
        parquet_df
          .select("ss_sold_time_sk", "ss_item_sk", "ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk")
          .na
          .drop()

      processed_parquet_df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", "ss_cdemo_sk,ss_hdemo_sk")
        .option("cubeSize", 300000)
        .save(qbeast_table_path)

      val qbeast_df = spark.read.format("qbeast").load(qbeast_table_path)

      val qbeastSnapshot = getQbeastSnapshot(qbeast_table_path)
      val totalNumberOfFiles = qbeastSnapshot.allFilesCount

      totalNumberOfFiles should be > 1L withClue
        "Total number of files in pushdown notebook changes to " + totalNumberOfFiles

      val query = qbeast_df.sample(0.1)
      val queryCount = query.count()
      val totalCount = qbeast_df.count()

      queryCount shouldBe totalCount / 10L +- totalCount / 100L withClue
        "The sample should be more or less 10%"
      import spark.implicits._

      val files = query.select(input_file_name()).distinct().as[String].collect()

      val numberOfFilesQuery = files.length.toLong
      numberOfFilesQuery should be < totalNumberOfFiles withClue
        "Number of files read in pushdown notebook changes to " + numberOfFilesQuery

      val numberOfRowsRead = spark.read.format("parquet").load(files: _*).count()

      numberOfRowsRead should be >= queryCount withClue
        "Number of rows read in pushdown notebook changes to " + numberOfRowsRead

    }
  }

}
