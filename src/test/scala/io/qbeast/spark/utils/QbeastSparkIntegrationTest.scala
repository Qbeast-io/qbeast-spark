package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class QbeastSparkIntegrationTest extends QbeastIntegrationTestSpec {

  def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(1, 2, 3, 4).toDF("id")
  }

  "The QbeastDataSource" should
    "work with DataFrame API" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createTestData(spark)
        data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

        val indexed = spark.read.format("qbeast").load(tmpDir)

        indexed.count() shouldBe 4

        indexed.columns shouldBe Seq("id")

        indexed.orderBy("id").collect() shouldBe data.orderBy("id").collect()

      }
    }

  it should "work with SaveAsTable" in withQbeastContextSparkAndTmpWarehouse { (spark, tmpDir) =>
    {

      val data = createTestData(spark)
      val location = tmpDir + "/external"
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("location", location)
        .saveAsTable("qbeast")

      val indexed = spark.read.format("qbeast").load(location)

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

    }
  }

  it should "support INSERT INTO on a managed Table" in
    withQbeastContextSparkAndTmpWarehouse { (spark, _) =>
      {
        import spark.implicits._

        val targetColumns = Seq("product_id", "brand", "price", "user_id")

        val initialData = loadTestData(spark).select(targetColumns.map(col): _*)
        val dataToInsert = Seq((1, "qbeast", 9.99, 1)).toDF(targetColumns: _*)

        initialData.write
          .format("qbeast")
          .option("cubeSize", "5000")
          .option("columnsToIndex", "user_id,product_id")
          .saveAsTable("ecommerce")

        dataToInsert.createOrReplaceTempView("toInsert")

        spark.sql("INSERT INTO ecommerce TABLE toInsert")
        spark.sql("SELECT * FROM ecommerce").count shouldBe 1 + initialData.count
        spark.sql("SELECT * FROM ecommerce WHERE brand == 'qbeast'").count shouldBe 1

      }
    }

  it should "support INSERT OVERWRITE on a managed Table" in
    withQbeastContextSparkAndTmpWarehouse { (spark, _) =>
      {
        import spark.implicits._

        val targetColumns = Seq("product_id", "brand", "price", "user_id")

        val initialData = loadTestData(spark).select(targetColumns.map(col): _*)
        val dataToInsert = Seq((1, "qbeast", 9.99, 1)).toDF(targetColumns: _*)

        initialData.write
          .format("qbeast")
          .option("cubeSize", "5000")
          .option("columnsToIndex", "user_id,product_id")
          .saveAsTable("ecommerce")

        dataToInsert.createOrReplaceTempView("toInsert")

        spark.sql("INSERT OVERWRITE ecommerce TABLE toInsert")
        spark.sql("SELECT * FROM ecommerce").count shouldBe 1
        spark.sql("SELECT * FROM ecommerce WHERE brand == 'qbeast'").count shouldBe 1

      }
    }
}
