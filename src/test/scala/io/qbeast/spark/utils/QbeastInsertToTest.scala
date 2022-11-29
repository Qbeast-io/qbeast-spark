package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col

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
        spark.sql("select * from t").collect() shouldBe initialData
          .union(insertDataLower)
          .collect()

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
        spark.sql("select * from t").collect() shouldBe initialData
          .union(insertDataLower)
          .collect()
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
        spark.sql("select * from t").collect() shouldBe initialData
          .union(Seq(4, 5).toDF())
          .collect()
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
        spark.sql("select * from t").collect() shouldBe initialData
          .union(Seq(4).toDF())
          .collect()
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

        spark.sql("SELECT * FROM initial").collect() shouldBe initialData
          .union(dataToInsert)
          .collect()
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

        spark.sql("SELECT * FROM initial").collect() shouldBe initialData
          .union(dataToInsert)
          .collect()
      }
  }

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

        spark.sql("SELECT * FROM initial").collect() shouldBe dataToInsert.collect()
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

        spark.sql("SELECT * FROM initial").collect() shouldBe dataToInsert.collect()
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

        spark.sql("SELECT * FROM initial").collect() shouldBe dataToInsert.collect()
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

        spark.sql("SELECT * FROM initial").collect() shouldBe dataToInsert.collect()
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

        spark.sql("SELECT * FROM initial").collect() shouldBe dataToInsert.collect()
      }
    }

}
