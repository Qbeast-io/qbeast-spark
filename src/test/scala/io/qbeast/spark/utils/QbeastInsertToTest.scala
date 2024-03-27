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
package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row

class QbeastInsertToTest extends QbeastIntegrationTestSpec {

  "Qbeast" should
    "support insert into using select " +
    "statement" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val cubeSize = 10000
        import spark.implicits._
        val initialData = Seq(5, 6, 7, 8).toDF("value")
        val insertDataLower = Seq(2).toDF("value")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("t")
        insertDataLower.createOrReplaceTempView("t_lower")

        // Insert using a SELECT statement
        spark.sql("insert into table t select * from t_lower")
        val dataInserted = spark.sql("SELECT * FROM t")
        assertSmallDatasetEquality(
          dataInserted,
          initialData.union(insertDataLower),
          orderedComparison = false,
          ignoreNullable = true)

      }
    }

  it should
    "support insert into using from " +
    "statement" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val cubeSize = 10000
        import spark.implicits._
        val initialData = Seq(5, 6, 7, 8).toDF("value")
        val insertDataLower = Seq(2).toDF("value")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("t")
        insertDataLower.createOrReplaceTempView("t_lower")

        // Insert using a FROM statement
        spark.sql("insert into table t from t_lower select *")
        val dataInserted = spark.sql("SELECT * FROM t")
        assertSmallDatasetEquality(
          dataInserted,
          initialData.union(insertDataLower),
          orderedComparison = false,
          ignoreNullable = true)
      }
    }

  it should
    "support insert into using multi-row values " +
    "statement" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val cubeSize = 10000
        import spark.implicits._
        val initialData = Seq(5, 6, 7, 8).toDF("value")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("t")

        // Multi-Row Insert Using a VALUES Clause
        spark.sql("insert into table t (value) values (4),(5)")
        val dataInserted = spark.sql("SELECT * FROM t")
        assertSmallDatasetEquality(
          dataInserted,
          initialData.union(Seq(4, 5).toDF()),
          orderedComparison = false,
          ignoreNullable = true)
      }
    }

  it should
    "support insert into using singe-row values " +
    "statement" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val cubeSize = 10000
        import spark.implicits._
        val initialData = Seq(5, 6, 7, 8).toDF("value")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("t")

        // Single Row Insert Using a VALUES Clause
        spark.sql("insert into table t (value) values (4)")
        val dataInserted = spark.sql("SELECT * FROM t")
        assertSmallDatasetEquality(
          dataInserted,
          initialData.union(Seq(4).toDF()),
          orderedComparison = false,
          ignoreNullable = true)
      }
    }

  it should "support INSERT INTO using a TABLE statement" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val cubeSize = 10000
        val initialData = Seq(5, 6, 7, 8).toDF("value")
        val dataToInsert = Seq(1, 2, 3, 4).toDF("value")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        df.createOrReplaceTempView("initial")
        dataToInsert.createOrReplaceTempView("toInsert")

        // Insert using a TABLE statement
        spark.sql("INSERT INTO initial TABLE toInsert")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          initialData.union(dataToInsert),
          orderedComparison = false,
          ignoreNullable = true)
      }
  }

  it should "support INSERT INTO with COLUMN LIST" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val cubeSize = 10000
        val initialData = Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4)).toDF("a", "b")
        val dataToInsert = Seq(("5", 5), ("6", 6)).toDF("a", "b")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "a,b", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        df.createOrReplaceTempView("initial")

        // Insert using a COLUMN LIST
        spark.sql("INSERT INTO initial (a, b) VALUES ('5', 5), ('6', 6)")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          initialData.union(dataToInsert),
          orderedComparison = false,
          ignoreNullable = true)
      }
  }

  it should "insert into table with no schema" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      spark.sql("INSERT INTO student VALUES (1, 'John', 10)")

      spark.table("student").count() shouldBe 1
      spark.table("student").head() shouldBe Row(1, "John", 10)
    })

  it should "support INSERT OVERWRITE using a VALUE clause" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val cubeSize = 10000
        val initialData = Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4)).toDF("a", "b")
        val dataToInsert = Seq(("5", 5), ("6", 6)).toDF("a", "b")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "a,b", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        df.createOrReplaceTempView("initial")

        // Overwrite using a VALUE clause
        spark.sql("INSERT OVERWRITE initial VALUES ('5', 5), ('6', 6)")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          dataToInsert,
          orderedComparison = false,
          ignoreNullable = true)
      }
  }

  it should "support INSERT OVERWRITE using a SELECT statement" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val cubeSize = 10000
        val initialData = Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4)).toDF("a", "b")
        val dataToInsert = Seq(("5", 5), ("6", 6)).toDF("a", "b")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "a,b", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        df.createOrReplaceTempView("initial")
        dataToInsert.createOrReplaceTempView("toInsert")

        // Overwrite using a SELECT statement
        spark.sql("INSERT OVERWRITE initial SELECT a, b FROM toInsert")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          dataToInsert,
          orderedComparison = false,
          ignoreNullable = true)
      }
  }

  it should "support INSERT OVERWRITE using a TABLE statement" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val cubeSize = 10000
        val initialData = Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4)).toDF("a", "b")
        val dataToInsert = Seq(("5", 5), ("6", 6)).toDF("a", "b")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "a,b", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        df.createOrReplaceTempView("initial")
        dataToInsert.createOrReplaceTempView("toInsert")

        // Overwrite using a TABLE statement
        spark.sql("INSERT OVERWRITE initial TABLE toInsert")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          dataToInsert,
          orderedComparison = false,
          ignoreNullable = true)
      }
  }

  it should "support INSERT OVERWRITE using a FROM statement" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        import spark.implicits._

        val cubeSize = 10000
        val initialData = Seq(("1", 1), ("2", 2), ("3", 3), ("4", 4)).toDF("a", "b")
        val dataToInsert = Seq(("5", 5), ("6", 6)).toDF("a", "b")

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "a,b", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)
        df.createOrReplaceTempView("initial")
        dataToInsert.createOrReplaceTempView("toInsert")

        // Overwrite using a FROM statement
        spark.sql("INSERT OVERWRITE initial FROM toInsert SELECT *")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          dataToInsert,
          orderedComparison = false,
          ignoreNullable = true)
      }
  }

  it should "support INSERT INTO using a TABLE statement on real data" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        import spark.implicits._

        val targetColumns = Seq("product_id", "brand", "price", "user_id")

        val initialData = loadTestData(spark).select(targetColumns.map(col): _*)
        val dataToInsert = Seq((1, "qbeast", 9.99, 1)).toDF(targetColumns: _*)

        initialData.write
          .format("qbeast")
          .option("cubeSize", "5000")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("initial")
        dataToInsert.createOrReplaceTempView("toInsert")

        // Insert using a TABLE statement on real data
        spark.sql("INSERT INTO initial TABLE toInsert")

        spark
          .sql("""SELECT * FROM initial
            |WHERE product_id == 1 and brand == 'qbeast' and
            |price == 9.99 and user_id == 1""".stripMargin.replaceAll("\n", " "))
          .count shouldBe 1

        spark.sql("SELECT * FROM initial").count shouldBe initialData.count + 1
      }
    }

  it should "support INSERT OVERWRITE using a TABLE statement on real data" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        import spark.implicits._

        val targetColumns = Seq("product_id", "brand", "price", "user_id")

        val initialData = loadTestData(spark).select(targetColumns.map(col): _*)
        val dataToInsert = Seq((1, "qbeast", 9.99, 1)).toDF(targetColumns: _*)

        initialData.write
          .format("qbeast")
          .option("cubeSize", "5000")
          .option("columnsToIndex", "user_id,product_id")
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("initial")
        dataToInsert.createOrReplaceTempView("toInsert")

        // Overwrite using a TABLE statement on real data
        spark.sql("INSERT OVERWRITE initial TABLE toInsert")

        val dataInserted = spark.sql("SELECT * FROM initial")
        assertSmallDatasetEquality(
          dataInserted,
          dataToInsert,
          orderedComparison = false,
          ignoreNullable = true)
      }
    }

}
