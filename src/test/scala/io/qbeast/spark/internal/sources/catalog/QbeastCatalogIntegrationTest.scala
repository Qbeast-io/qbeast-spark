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
package io.qbeast.spark.internal.sources.catalog

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.AnalysisException
import org.apache.spark.SparkConf

class QbeastCatalogIntegrationTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "QbeastCatalog" should
    "coexist with Delta tables" in withTmpDir(tmpDir =>
      withExtendedSpark(sparkConf = new SparkConf()
        .setMaster("local[8]")
        .set("spark.sql.extensions", "io.qbeast.spark.internal.QbeastSparkSessionExtension")
        .set("spark.sql.warehouse.dir", tmpDir)
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set(
          "spark.sql.catalog.qbeast_catalog",
          "io.qbeast.spark.internal.sources.catalog.QbeastCatalog"))(spark => {

        val data = createTestData(spark)

        data.write.format("delta").saveAsTable("delta_table") // delta catalog

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_catalog.default.qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table("delta_table")
        val qbeastTable = spark.read.table("qbeast_catalog.default.qbeast_table")

        assertSmallDatasetEquality(
          deltaTable,
          qbeastTable,
          orderedComparison = false,
          ignoreNullable = true)

      }))

  it should
    "coexist with Delta tables in the same catalog" in withQbeastContextSparkAndTmpWarehouse(
      (spark, _) => {

        val data = createTestData(spark)

        data.write.format("delta").saveAsTable("delta_table") // delta catalog

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table("delta_table")
        val qbeastTable = spark.read.table("qbeast_table")

        assertSmallDatasetEquality(
          deltaTable,
          qbeastTable,
          orderedComparison = false,
          ignoreNullable = true)

      })

  it should "crate table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='id')")

    val table = spark.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    // Create table first (must be in qbeast format)
    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='age')")

    spark.sql("SHOW TABLES").count() shouldBe 1

    // Try to replace it
    spark.sql(
      "REPLACE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='age')")

    spark.sql("SHOW TABLES").count() shouldBe 1

    val table = spark.read.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "create or replace table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
        " USING qbeast OPTIONS ('columnsToIndex'='id')")

    val table = spark.read.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "create table and insert data as select" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        "CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')")

      import spark.implicits._
      // Create temp view with data to try SELECT AS statement
      students.toDF.createOrReplaceTempView("bronze_student")

      spark.sql("INSERT INTO table student SELECT * FROM bronze_student")
      spark.sql("SELECT * FROM student").count() shouldBe students.size

      spark.sql("INSERT INTO table student TABLE bronze_student")
      spark.sql("SELECT * FROM student").count() shouldBe students.size * 2

    })

  it should "crate external table" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      val tmpDir = tmpWarehouse + "/test"
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      spark.sql(
        "CREATE EXTERNAL TABLE student " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpDir'")

      val table = spark.table("student")
      val indexed = spark.read.format("qbeast").load(tmpDir)
      assertSmallDatasetEquality(table, indexed, orderedComparison = false)

    })

  it should "crate external table with the schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      val tmpDir = tmpWarehouse + "/test"
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      spark.sql(
        "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpDir'")

      val table = spark.table("student")
      val indexed = spark.read.format("qbeast").load(tmpDir)
      assertSmallDatasetEquality(table, indexed, orderedComparison = false)

    })

  it should "throw error when the specified schema mismatch existing schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      val tmpDir = tmpWarehouse + "/test"
      val data = createTestData(spark)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("CREATE EXTERNAL TABLE student (id INT, age INT) " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpDir'"))

    })

  it should "throw error when no schema and no populated table" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("CREATE EXTERNAL TABLE student " +
          s"USING qbeast OPTIONS ('columnsToIndex'='id') LOCATION '$tmpWarehouse'"))

    })

  it should "throw an error when trying to replace a non-qbeast table" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT)" +
          " USING parquet")

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')"))

    })

  it should "throw an error when replacing non-existing table" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')"))

    })

  it should "throw an error when using partitioning/bucketing" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id') PARTITIONED BY (id)"))

    })

  it should "persist altered properties on the _delta_log" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      val df = loadTestData(spark)
      df.write.format("qbeast").option("columnsToIndex", "user_id,price").save(tmpDir)
      spark.sql(s"CREATE TABLE t1 USING qbeast LOCATION '$tmpDir'")
      spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")

      // Check the delta log info
      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val properties = snapshot.getProperties

      properties should contain key "k"
      properties("k") shouldBe "v"

    })

  it should "persist ALL original properties of table" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql(
        s"CREATE TABLE t1(id INT) USING qbeast TBLPROPERTIES('k' = 'v', 'columnsToIndex' = 'id')")

      // Check the delta log info
      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val properties = snapshot.getProperties

      properties should contain key "columnsToIndex"
      properties should contain key "k"
      properties("columnsToIndex") shouldBe "id"
      properties("k") shouldBe "v"

    })

}
