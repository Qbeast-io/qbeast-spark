package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row

/**
 * Test for checking the correctness of the output schemas when Appending Data through INSERT INTO
 */
class QbeastSchemaTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "detect when schemas does not match on INSERT INTO" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      an[AnalysisException] shouldBe thrownBy(spark.sql("INSERT INTO student VALUES (1, 'John')"))

    })

  it should "replace schemas by ordinal" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    spark.sql("INSERT INTO student VALUES (1, 'John', 10)")

    spark.table("student").head() shouldBe Row(1, "John", 10)

  })

  it should "replace schemas by name" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    spark.sql("INSERT INTO student(id, name, age) VALUES (1, 'John', 10)")

    spark.table("student").head() shouldBe Row(1, "John", 10)
  })

  it should "detect schema mismatch" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    an[AnalysisException] shouldBe thrownBy(
      spark.sql("INSERT INTO student(id1, name2, age3) VALUES (1, 'John', 10)"))

  })

  it should "fail when types does not match" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      an[Exception] shouldBe thrownBy(spark.sql("INSERT INTO student VALUES ('John', 10, 1)"))

    })

  it should "replace schemas from other table" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      spark.sql(
        "CREATE TABLE student_parquet (id_parquet INT, name_parquet STRING, age_parquet INT) " +
          "USING parquet")
      spark.sql("INSERT INTO student_parquet VALUES (1, 'John', 10)")

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      spark.sql("INSERT INTO student SELECT * FROM student_parquet")

      spark.table("student").head() shouldBe Row(1, "John", 10)
    })

  it should "not replace schemas from other table if the number do not match" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        "CREATE TABLE student_parquet (id_parquet INT, name_parquet STRING) " +
          "USING parquet")
      spark.sql("INSERT INTO student_parquet VALUES (1, 'John')")

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("INSERT INTO student SELECT * FROM student_parquet"))

    })

  it should "not replace schemas from other table if the type do not match" in
    withQbeastContextSparkAndTmpWarehouse((spark, _) => {

      spark.sql(
        "CREATE TABLE student_parquet (id STRING, name STRING, age INT) " +
          "USING parquet")
      spark.sql("INSERT INTO student_parquet VALUES ('1', 'John', 20L)")

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING delta " +
          "OPTIONS ('columnsToIndex'='id')")

      an[AnalysisException] shouldBe thrownBy(
        spark.sql("INSERT INTO student SELECT * FROM student_parquet"))

    })

  it should "work with delta" in withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

    spark.sql(s"CREATE TABLE student (id INT) USING delta LOCATION '$tmpDir/student'")
    val location = s"$tmpDir/student"
    // read delta
    spark.read.format("delta").load(location).show()

  })

  it should "not merge schemas if specified in SQL" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      // TODO: all failing because when creating a table, no initial schema is commited

      import spark.implicits._

      spark.sql(
        "CREATE TABLE student (id INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      val dfExtraCol = Seq((1, "John"), (2, "Doe")).toDF("id", "name")

      // EXRA COLUMN
      an[AnalysisException] shouldBe thrownBy(
        dfExtraCol.write
          .format("qbeast")
          .mode("append")
          .option("columnsToIndex", "id")
          .insertInto("student"))

      // RENAME
      val renamedDF = Seq((1, "John", 10)).toDF("id", "name2", "age")
      an[AnalysisException] shouldBe thrownBy(
        renamedDF.write
          .format("qbeast")
          .mode("append")
          .option("columnsToIndex", "id")
          .insertInto("student"))

    })

  it should "not merge schemas if specified with DataFrame API" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._

      val df = Seq(1, 2).toDF("id")
      val path = s"$tmpDir/student"
      df.write
        .format("qbeast")
        .mode("overwrite")
        .option("columnsToIndex", "id")
        .save(path)

      val dfExtraCol = Seq((1, "John"), (2, "Doe")).toDF("id", "name")

      // EXRA COLUMN
      an[AnalysisException] shouldBe thrownBy(
        dfExtraCol.write
          .format("qbeast")
          .mode("append")
          .option("columnsToIndex", "id")
          .save(path))

      // RENAME
      val renamedDF = Seq((1, "John", 10)).toDF("id", "name2", "age")
      an[AnalysisException] shouldBe thrownBy(
        renamedDF.write
          .format("qbeast")
          .mode("append")
          .option("columnsToIndex", "id")
          .save(path))

      // MERGESCHEMA YES
      dfExtraCol.write
        .format("qbeast")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)

      spark.read
        .format("qbeast")
        .load(path)
        .schema
        .fieldNames shouldBe dfExtraCol.schema.fieldNames

    })

}
