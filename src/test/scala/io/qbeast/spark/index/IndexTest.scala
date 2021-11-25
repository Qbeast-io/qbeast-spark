/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.TestClasses.{Client3, Client4}
import io.qbeast.model._
import io.qbeast.spark.index.IndexTestChecks._
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import io.qbeast.spark.utils.TagUtils
import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.SparkException
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

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

  "Indexing method" should "respect the desired cube size" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (indexed, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubeSize(tc, rev, indexed)
      }
    }
  }

  it should "not miss any cube" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubes(tc.indexChanges.cubeWeights)
      }
    }
  }

  it should "respect the weight of the fathers" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkWeightsIncrement(tc.indexChanges.cubeWeights)
      }
    }
  }

  it should "add only leaves to indexed data" in withSpark { _ =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (indexed, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubesOnData(tc.indexChanges.cubeWeights, indexed, 2)
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
        val (indexed, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))
        val weightMap = tc.indexChanges.cubeWeights
        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, 2)
        checkCubeSize(tc, rev, indexed)

      }
    }
  }

  it should "maintain correctness on append" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF()
        val tableId = new QTableID(tmpDir)
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

        val (i, _) = oTreeAlgorithm.index(df, IndexStatus(rev))
        val firstIndexed = i.withColumnRenamed(cubeColumnName, cubeColumnName + "First")

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

        val offset = 0.5
        val appendData = df
          .withColumn("age", (col("age") * offset).cast(IntegerType))
          .withColumn("val2", (col("val2") * offset).cast(LongType))

        val appendRev =
          SparkRevisionFactory.createNewRevision(tableId, appendData.schema, options)
        val (indexed, tc) =
          oTreeAlgorithm.index(appendData, qbeastSnapshot.loadLatestIndexStatus)

        checkCubes(tc.indexChanges.cubeWeights)
        checkWeightsIncrement(tc.indexChanges.cubeWeights)
        checkCubesOnData(tc.indexChanges.cubeWeights, indexed, 2)
        checkCubeSize(
          tc,
          appendRev,
          indexed.join(
            firstIndexed,
            firstIndexed(cubeColumnName + "First") === indexed(cubeColumnName)))

      }

  }

  it should "throw an exception on null values" in withSpark { spark =>
    {
      withOTreeAlgorithm { oTreeAlgorithm =>
        val rdd =
          spark.sparkContext.parallelize(
            0.to(2)
              .map(i => {
                if (i % 2 == 0) {
                  Client4(
                    i * i,
                    s"student-$i",
                    Some(i),
                    Some(i * 1000 + 123),
                    Some(i * 2567.3432143))
                } else Client4(i * i, s"student-$i", None, None, None)
              }))
        val df = spark.createDataFrame(rdd)

        try {
          val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)
          oTreeAlgorithm.index(df, IndexStatus(rev))
          fail()
        } catch {
          case e: SparkException if e.getCause.isInstanceOf[AnalysisException] =>
        }
      }
    }
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

        deltaLog.snapshot.allFiles.collect() foreach (f =>
          {
            val cubeId = CubeId(2, f.tags("cube"))
            cubeId.parent match {
              case None => // cube is root
              case Some(parent) =>
                val minWeight = Weight(f.tags(TagUtils.minWeight).toInt)
                val parentMaxWeight = tc.indexChanges.cubeWeights(parent)

                minWeight should be >= parentMaxWeight
            }
          }: Unit)
      }
    }

}
