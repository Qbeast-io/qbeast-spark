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
package io.qbeast.catalog

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.AnalysisException
import org.apache.spark.SparkConf

class QbeastCatalogIntegrationTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "QbeastCatalog" should
    "coexist with the table-format catalog" in withTmpDir(tmpDir =>
      withExtendedSpark(sparkConf = new SparkConf()
        .setMaster("local[8]")
        .set("spark.sql.warehouse.dir", tmpDir)
        .set("spark.sql.extensions", "io.qbeast.sql.QbeastSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.catalog.qbeast_catalog", "io.qbeast.catalog.QbeastCatalog"))(spark => {

        val data = createTestData(spark)

        val table_name = s"${DEFAULT_TABLE_FORMAT}_table"
        data.write.format(DEFAULT_TABLE_FORMAT).saveAsTable(table_name)

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_catalog.default.qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table(table_name)
        val qbeastTable = spark.read.table("qbeast_catalog.default.qbeast_table")

        assertSmallDatasetEquality(
          deltaTable,
          qbeastTable,
          orderedComparison = false,
          ignoreNullable = true)

      }))

  it should
    "coexist with table in the same catalog" in withQbeastContextSparkAndTmpWarehouse(
      (spark, _) => {

        val data = createTestData(spark)

        val table_name = s"${DEFAULT_TABLE_FORMAT}_table"
        data.write.format(DEFAULT_TABLE_FORMAT).saveAsTable(table_name) // table format catalog

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .saveAsTable("qbeast_table") // qbeast catalog

        val tables = spark.sessionState.catalog.listTables("default")
        tables.size shouldBe 2

        val deltaTable = spark.read.table(table_name)
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

  it should "persist altered properties on the table log" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql("CREATE TABLE t1(id INT) USING qbeast TBLPROPERTIES ('columnsToIndex'= 'id')")
      spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")

      val tableIdentifier = TableIdentifier("t1")
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(tableIdentifier)

      val snapshot = getQbeastSnapshot(tableMetadata.location.toString)
      val properties = snapshot.loadProperties

      properties should contain key "k"
      properties("k") shouldBe "v"

    })

  it should "persist UNSET properties" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE t1(id INT) " +
        "USING qbeast " +
        "TBLPROPERTIES ('columnsToIndex'= 'id')")

    spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")

    val tableIdentifier = TableIdentifier("t1")
    val tableMetadata = spark.sessionState.catalog.getTableMetadata(tableIdentifier)

    val snapshot = getQbeastSnapshot(tableMetadata.location.toString)
    val properties = snapshot.loadProperties

    properties should contain key "k"
    properties("k") shouldBe "v"

    spark.sql("ALTER TABLE t1 UNSET TBLPROPERTIES ('k')")

    val updatedSnapshot = getQbeastSnapshot(tableMetadata.location.toString)
    val updatedProperties = updatedSnapshot.loadProperties
    updatedProperties should not contain key("k")
  })

  it should "ensure consistency with the session catalog" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      import spark.implicits._

      spark.sql(
        "CREATE TABLE t1(id INT) " +
          "USING qbeast " +
          "TBLPROPERTIES ('columnsToIndex'= 'id')")
      spark.sql("ALTER TABLE t1 SET TBLPROPERTIES ('k' = 'v')")

      val tableIdentifier = TableIdentifier("t1")
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(tableIdentifier)

      val snapshot = getQbeastSnapshot(tableMetadata.location.toString)
      val properties = snapshot.loadProperties

      val showProperties = spark.sql("SHOW TBLPROPERTIES t1").as[(String, String)].collect().toMap

      val catalog = spark.sessionState.catalog
      val catalogProperties = catalog.getTableMetadata(tableIdentifier).properties

      properties should contain key "k"
      catalogProperties should contain key "k"
      showProperties should contain key "k"

      spark.sql("ALTER TABLE t1 UNSET TBLPROPERTIES ('k')")

      val updatedSnapshot = getQbeastSnapshot(tableMetadata.location.toString)
      val updatedProperties = updatedSnapshot.loadProperties
      val updatedCatalogProperties =
        catalog.getTableMetadata(tableIdentifier).properties
      val updatedShowProperties =
        spark.sql("SHOW TBLPROPERTIES t1").as[(String, String)].collect().toMap

      updatedProperties should not contain key("k")
      updatedCatalogProperties should not contain key("k")
      updatedShowProperties should not contain key("k")
    })

  it should "persist ALL original properties of table" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql(
        s"CREATE TABLE t1(id INT) USING qbeast LOCATION '$tmpDir' " +
          "TBLPROPERTIES('k' = 'v', 'columnsToIndex' = 'id')")

      val snapshot = getQbeastSnapshot(tmpDir)
      val properties = snapshot.loadProperties

      properties should contain key "columnsToIndex"
      properties should contain key "k"
      properties("columnsToIndex") shouldBe "id"
      properties("k") shouldBe "v"

    })

}
