package io.qbeast.spark.internal.sources

import io.qbeast.TestClasses.Student
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

class QbeastCatalogTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  private val schema = StructType(
    Seq(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

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

        // spark.sql("USE CATALOG qbeast_catalog")
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

  it should "crate table" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast OPTIONS ('columnsToIndex'='id')")

    val table = spark.table("student")
    table.schema shouldBe schema
    table.count() shouldBe 0

  })

  it should "replace table" in withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

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

  it should "create or replace table" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql(
        "CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
          " USING qbeast OPTIONS ('columnsToIndex'='id')")

      val table = spark.read.table("student")
      table.schema shouldBe schema
      table.count() shouldBe 0

    })

  // TODO this test fails because the data has schema (col1, col2, col3) Check how to solve it
//  it should "create table and insert data" in withQbeastContextSparkAndTmpWarehouse(
//    (spark, tmpDir) => {
//
//      spark.sql(
//        "CREATE OR REPLACE TABLE student (id INT, name STRING, age INT)" +
//          " USING qbeast OPTIONS ('columnsToIndex'='id')")
//
//      // Insert one single element
//      spark.sql("INSERT INTO table student values (4, 'Joan', 20)")
//      val table = spark.sql("SELECT * FROM student")
//      table.schema shouldBe schema
//      table.count() shouldBe 1
//
//    })

  it should "create table and insert data as select" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

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

}
