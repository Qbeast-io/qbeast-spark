package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

class QbeastDifferentProviderTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  // TODO
  "QbeastSpark SQL" should "support CREATE TABLE" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {
      val data = createTestData(spark)
      data.createOrReplaceTempView("data")

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      val nonTemporaryTables = spark.sql("SHOW TABLES FROM default")
      nonTemporaryTables.count() shouldBe 2 // data table and student table

      val table = spark.sql("DESCRIBE TABLE EXTENDED student")
      // TODO Check the metadata of the table
      // Check provider
      table
        .where("col_name == 'Provider'")
        .select("data_type")
        .first()
        .getString(
          0) shouldBe "delta" // TODO: Trying to change the provider to the original table format
      // Check Table Properties
      val tableProperties = table
        .where("col_name == 'Table Properties'")
        .select("data_type")
        .first()
        .getString(0)
      tableProperties should include("columnsToIndex=id")
      tableProperties should include("option.columnsToIndex=id")

    })

}
