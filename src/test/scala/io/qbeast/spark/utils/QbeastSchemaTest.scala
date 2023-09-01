package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.{AnalysisException, Row}

/**
 * Test for checking the correctness of the output schemas
 * when Appending Data through INSERT INTO
 */
class QbeastSchemaTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "detect when schemas does not match on INSERT INTO" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      an[AnalysisException] shouldBe thrownBy(spark.sql("INSERT INTO student VALUES (1, 'John')"))

    })

  it should "replace schemas by ordinal" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    spark.sql("INSERT INTO student VALUES (1, 'John', 10)")

    spark.table("student").head() shouldBe Row(1, "John", 10)

  })

  it should "replace schemas by name" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    spark.sql("INSERT INTO student(id, name, age) VALUES (1, 'John', 10)")

    spark.table("student").head() shouldBe Row(1, "John", 10)
  })

  it should "detect schema mismatch" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    an[AnalysisException] shouldBe thrownBy(
      spark.sql("INSERT INTO student(id1, name2, age3) VALUES (1, 'John', 10)"))

    spark.table("student").head() shouldBe Row(1, "John", 10)
  })

  it should "fail when types does not match" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      an[Exception] shouldBe thrownBy(spark.sql("INSERT INTO student VALUES ('John', 10, 1)"))

    })

  it should "replace schemas from other table" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        s"CREATE TABLE student_parquet (id_parquet INT, name_parquet STRING, age_parquet INT) " +
          s"USING parquet")
      spark.sql("INSERT INTO student_parquet VALUES (1, 'John', 10)")

      spark.sql(
        s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      spark.sql("INSERT INTO student SELECT * FROM student_parquet")

      spark.table("student").head() shouldBe Row(1, "John", 10)
    })

  it should "not replace schemas from other table if they do not match" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        s"CREATE TABLE student_parquet (id_parquet INT, name_parquet STRING) " +
          s"USING parquet")
      spark.sql("INSERT INTO student_parquet VALUES (1, 'John')")

      spark.sql(
        s"CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("INSERT INTO student SELECT * FROM student_parquet"))

    })
}
