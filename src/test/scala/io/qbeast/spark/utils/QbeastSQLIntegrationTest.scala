package io.qbeast.spark.utils

import io.qbeast.TestClasses.Student
import io.qbeast.spark.QbeastIntegrationTestSpec

import scala.util.Random

class QbeastSQLIntegrationTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  "QbeastSpark" should "work with SQL CREATE TABLE" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      import spark.implicits._
      val data = students.toDF()
      data.createOrReplaceTempView("data")

      spark.sql(
        s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      val nonTemporaryTables = spark.sql("SHOW TABLES FROM default")
      nonTemporaryTables.count() shouldBe 2 // data table and student table

      val table = spark.sql("DESCRIBE TABLE EXTENDED student")
      table.show(false)
      // Check provider
      table
        .where("col_name == 'Provider'")
        .select("data_type")
        .first()
        .getString(0) shouldBe "qbeast"
      // Check Location
      table
        .where("col_name == 'Location'")
        .select("data_type")
        .first()
        .getString(0) shouldBe tmpWarehouse + "/student"
      // Check Table Properties
      table
        .where("col_name == 'Storage Properties'")
        .select("data_type")
        .first()
        .getString(0) shouldBe "[columnsToIndex=id]"

    })

  it should "work with INSERT INTO" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    import spark.implicits._
    val data = students.toDF()
    data.createOrReplaceTempView("data")

    spark.sql(
      s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    spark.sql("INSERT INTO table student SELECT * FROM data") // FAILS
  })

  it should "work with CREATE TABLE AS SELECT statement" in withQbeastContextSparkAndTmpDir(
    (spark, _) => {

      import spark.implicits._
      val data = students.toDF()
      data.createOrReplaceTempView("data")

      spark.sql(
        s"CREATE OR REPLACE TABLE student USING qbeast " +
          "OPTIONS ('columnsToIndex'='id') " +
          "AS SELECT * FROM data;")
    })

  it should "work with LOCATION" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val data = students.toDF()
    data.createOrReplaceTempView("data")

    spark.sql(
      s"CREATE OR REPLACE TABLE student USING qbeast " +
        "OPTIONS ('columnsToIndex'='id') " +
        s"LOCATION '$tmpDir' " +
        "AS SELECT * FROM data;")
  })

}
