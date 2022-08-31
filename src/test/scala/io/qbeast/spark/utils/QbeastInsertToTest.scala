package io.qbeast.spark.utils

import io.qbeast.spark.{QbeastIntegrationTestSpec}

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

        // TODO there might be more types of insert statements, but the most important
        // ones are the ones above
      }
    }

}
