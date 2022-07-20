package io.qbeast.spark.utils

import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.delta.DeltaLog

class QbeastInsertToTest extends QbeastIntegrationTestSpec {

  it should
    "insert data to the original dataset" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val cubeSize = 10000
        import spark.implicits._
        val initialData = Seq(5, 6, 7, 8).toDF("value")
        val insertDataLower = Seq(1, 2, 3, 4).toDF("value")
        val insertDataInner = Seq(6, 7).toDF("value")
        val insertDataHigher = Seq(9, 10, 11, 12).toDF("value")

        // function returning a boolean indicating whether the new inserted data respects the
        // new weight.MinValue and weight.MaxValue of the cubes
        def insert_respects_space(initialData: DataFrame, newData: DataFrame): Boolean = {
          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
          val cubeWeights = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

          val testWeight = initialData
            .select("value")
            .union(newData)
            .agg("value" -> "max")

          // scalastyle:off println
          Console.println("Hello")
          Console.println(testWeight.collect().toList(0).get(0))
          // scalastyle:on println
          // Get the max value from initialData and newData DataFrames
          val weightMax = initialData
            .select("value")
            .union(newData)
            .agg("value" -> "max")
            .collect()(0)
            .get(0)
          // val maxValue = initialData.select("value").agg("max").collect()(0).getLong(0)
          // val newMaxValue = newData.select("value").agg("max").collect()(0).getLong(0)
          // val weightMax = math.max(maxValue, newMaxValue)
          // Get the min value from initialData and newData DataFrames
          val weightMin = initialData
            .select("value")
            .union(newData)
            .agg("value" -> "min")
            .collect()(0)
            .get(0)
          // val minValue = initialData.select("value").agg("min").collect()(0).getLong(0)
          // val newMinValue = newData.select("value").agg("min").collect()(0).getLong(0)
          // val weightMin = math.min(minValue, newMinValue)
          // TODO weightMin and weightMax should be a hash, not the values
          cubeWeights.values.foreach { cubes =>
            cubes.files.map(_.minWeight).min shouldBe (weightMin)
            cubes.files.map(_.maxWeight).max shouldBe (weightMax)
          }
          true
        }

        initialData.write
          .mode("append")
          .format("qbeast")
          .options(Map("columnsToIndex" -> "value", "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        df.createOrReplaceTempView("t")
        insertDataLower.createOrReplaceTempView("t_lower")
        insertDataHigher.createOrReplaceTempView("t_higher")
        insertDataInner.createOrReplaceTempView("t_inner")

        // Insert using a SELECT statement
        spark.sql("insert into table t select * from t_lower")

        // Insert using a FROM statement
        spark.sql("insert into table t from t_inner select *")

        // Single Row Insert Using a VALUES Clause
        spark.sql("insert into table t (value) values (4)")

        // Multi-Row Insert Using a VALUES Clause
        spark.sql("insert into table t (value) values (4),(5)")

        // Reinitialize table t
        spark.sql("drop table t")
        df.createOrReplaceTempView("t")

        // Assert that the min weight is the min of the df
        spark.sql("insert into table t select * from t_lower")
        insert_respects_space(df, insertDataLower) shouldBe true

        // Reinitialize table t
        spark.sql("drop table t")
        df.createOrReplaceTempView("t")

        // Assert that the max weight is the max of the df
        spark.sql("insert into table t select * from t_higher")
        insert_respects_space(df, insertDataHigher) shouldBe true

        // Reinitialize table t
        spark.sql("drop table t")
        df.createOrReplaceTempView("t")

        // Assert that the min and max weight is not updated when new data is within
        // the current weight range
        spark.sql("insert into table t select * from t_inner")
        insert_respects_space(df, insertDataInner) shouldBe true

        df.count()

      }
    }
}
