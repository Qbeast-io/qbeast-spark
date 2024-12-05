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
package io.qbeast.spark.delta

import io.qbeast.core.model.IndexFile
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row

/**
 * Test for checking the correctness of the output schemas when Appending Data through INSERT INTO
 */
class QbeastSchemaDeltaTest extends QbeastIntegrationTestSpec {

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

      import spark.implicits._

      spark.sql(
        "CREATE TABLE student (id INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      val dfExtraCol = Seq((1, "John"), (2, "Doe")).toDF("id", "name")

      // EXTRA COLUMN
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

      // EXTRA COLUMN
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

  "loadDataframeFromIndexFiles" should "work properly with evolving schema" in {
    withSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val cubeSize = 100
      val options =
        Map("columnsToIndex" -> "id", "cubeSize" -> cubeSize.toString)
      spark
        .range(100)
        .filter("id % 2 = 0")
        .write
        .format("qbeast")
        .mode("overwrite")
        .options(options)
        .save(tmpDir)

      spark
        .range(100)
        .filter("id % 2 = 1")
        .withColumn("rand", rand())
        .write
        .option("mergeSchema", "true")
        .format("qbeast")
        .mode("append")
        .options(options)
        .save(tmpDir)

      spark.read.format("qbeast").load(tmpDir).count.toInt shouldBe 100

      val qbeastSnapshot = getQbeastSnapshot(tmpDir)

      val filesToOptimize = qbeastSnapshot.loadAllRevisions
        .map(rev => qbeastSnapshot.loadIndexFiles(rev.revisionID))
        .foldLeft(spark.emptyDataset[IndexFile])(_ union _)
      val allData = qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize)
      allData.count.toInt shouldBe 100

      val data = spark.read.format("qbeast").load(tmpDir)
      // I'm selecting only the old file with the first schema with only 1 column
      val filePath =
        data.filter("id % 2 = 0").select(input_file_name()).distinct().as[String].first()

      spark.read.parquet(filePath).columns shouldBe Seq("id")

      val fileName = new Path(filePath).getName

      val subSet =
        qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize.filter(_.path == fileName))

      subSet.schema shouldBe data.schema
      subSet.columns shouldBe Seq("id", "rand")

    }
  }

}
