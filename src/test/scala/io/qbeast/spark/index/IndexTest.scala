/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.TestClasses.{Client3, Client4}
import io.qbeast.core.model.{BroadcastedTableChanges, _}
import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec
    with IndexTestChecks {

  // TEST CONFIGURATIONS
  private val options = Map("columnsToIndex" -> "age,val2", "cubeSize" -> "10000")

  private def createDF(): DataFrame = {
    val spark = SparkSession.active

    val rdd =
      spark.sparkContext.parallelize(
        0.to(100000)
          .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))

    spark.createDataFrame(rdd)
  }

  // Check correctness

  "Indexing method" should "respect the size of the data" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (indexed, _) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkDFSize(indexed, df)
      }
    }
  }

  it should "not miss any cube" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (_, tc: BroadcastedTableChanges) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubes(tc.cubeWeights.value)
      }
    }
  }

  it should "respect the weight of the fathers" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (_, tc: BroadcastedTableChanges) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkWeightsIncrement(tc.cubeWeights.value)
      }
    }
  }

  it should "add only leaves to indexed data" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (indexed, tc: BroadcastedTableChanges) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubesOnData(tc.cubeWeights.value, indexed, dimensionCount = 2)
      }
    }
  }

  it should "work with real data" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val inputPath = "src/test/resources/"
        val file1 = "ecommerce100K_2019_Oct.csv"
        val df = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(inputPath + file1)
          .distinct()

        val rev = SparkRevisionFactory.createNewRevision(
          QTableID("test"),
          df.schema,
          Map("columnsToIndex" -> "user_id,product_id", "cubeSize" -> "10000"))
        val (indexed, tc: BroadcastedTableChanges) = oTreeAlgorithm.index(df, IndexStatus(rev))
        val weightMap = tc.cubeWeights.value

        checkDFSize(indexed, df)
        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, 2)

      }
    }
  }

  it should "maintain correctness on append" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF()

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.update())

        val offset = 0.5
        val appendData = df
          .withColumn("age", (col("age") * offset).cast(IntegerType))
          .withColumn("val2", (col("val2") * offset).cast(LongType))

        val (indexed, tc: BroadcastedTableChanges) =
          oTreeAlgorithm.index(appendData, qbeastSnapshot.loadLatestIndexStatus)
        val weightMap = tc.cubeWeights.value

        checkDFSize(indexed, df)
        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, 2)

      }

  }

  it should "support null values" in withSparkAndTmpDir { (spark, tmpDir) =>
    val clients = Seq(
      Client4(0, "student-0", Some(0), Some(123), Some(4.5)),
      Client4(1, "student-1", Some(6), Some(789), Some(0.1)),
      Client4(2, "student-2", None, None, None))
    val rdd = spark.sparkContext.makeRDD(clients)
    spark
      .createDataFrame(rdd)
      .write
      .format("qbeast")
      .mode("overwrite")
      .option("columnsToIndex", "age,val2")
      .save(tmpDir)

    val anotherClients = Seq(
      Client4(3, "student-3", Some(2), Some(345), Some(6.7)),
      Client4(4, "student-4", None, None, None),
      Client4(3, "student-5", Some(3), Some(349), Some(10.5)))
    val anotherRdd = spark.sparkContext.makeRDD(anotherClients)
    spark
      .createDataFrame(anotherRdd)
      .write
      .format("qbeast")
      .mode("append")
      .option("columnsToIndex", "age,val2")
      .save(tmpDir)
  }

  it should "follow the rule of children's minWeight >= parent's maxWeight" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)
        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val blocks = deltaLog
          .update()
          .allFiles
          .collect()
          .map(delta.IndexFiles.fromAddFile(2))
          .flatMap(_.blocks)
        blocks.foreach { block =>
          block.cubeId.parent match {
            case None => // cube is root
            case Some(parent) =>
              val parentMaxWeight = tc.cubeWeight(parent).get
              block.minWeight should be >= parentMaxWeight
          }
        }
      }
    }

  it should "index correctly when a small cubeSize is given" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      val rdd =
        spark.sparkContext.parallelize(
          0.to(1000)
            .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))

      val df = spark.createDataFrame(rdd)
      val smallCubeSize = 10
      val rev = SparkRevisionFactory.createNewRevision(
        QTableID("test"),
        df.schema,
        Map("columnsToIndex" -> "age,val2", "cubeSize" -> smallCubeSize.toString))

      val (indexed, tc: BroadcastedTableChanges) = oTreeAlgorithm.index(df, IndexStatus(rev))
      val weightMap = tc.cubeWeights.value

      checkDFSize(indexed, df)
      checkCubes(weightMap)
      checkWeightsIncrement(weightMap)
      checkCubesOnData(weightMap, indexed, 2)
    }
  }

}
