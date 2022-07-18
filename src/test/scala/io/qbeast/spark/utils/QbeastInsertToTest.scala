package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec

class QbeastInsertToTest extends QbeastIntegrationTestSpec {

  it should
    "insert data to the original dataset" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        // scalastyle:off println
        Console.println("Hello")
        // scalastyle:on println
        // val columnsToIndex = Seq("user_id", "product_id")
        val cubeSize = 10000
        // writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        // val insertData = spark.read
        // .format("csv")
        // .option("header", "true")
        // .option("inferSchema", "true")
        // .load("src/test/resources/ecommerce300k_2019_Nov.csv")

        import spark.implicits._
        val insertData = Seq(1, 2, 3).toDF("value")
        val insertData2 = Seq(4, 5, 6).toDF("value")

        insertData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        insertData2.createOrReplaceTempView("t2")
        df.createOrReplaceTempView("t")
        // spark.sql("select * from t")
        // spark.sql("insert into table t select * from t2")

        // TODO this syntax is currently not supported with qbeast, we should support it
        spark.sql("insert into table t (value) values (4)")

        df.count()

      }
    }
}
