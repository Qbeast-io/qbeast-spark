/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model._
import io.qbeast.spark.index.OTreeAlgorithmTest.{Client3, Client4}
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import io.qbeast.spark.{QbeastIntegrationTestSpec, SparkRevisionBuilder, delta}
import org.apache.spark.SparkException
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  def checkCubeSize(tableChanges: TableChanges, revision: Revision, indexed: DataFrame): Unit = {
    val weightMap: Map[CubeId, Weight] = tableChanges.indexChanges.cubeWeights
    val cubeSizes = indexed
      .groupBy(cubeColumnName)
      .count()
      .collect()
      .map(row =>
        (revision.createCubeId(row.getAs[Array[Byte]](cubeColumnName)), row.getAs[Long]("count")))
      .toMap

    cubeSizes.foreach { case (cubeId: CubeId, size: Long) =>
      weightMap.get(cubeId) match {
        case Some(weight) =>
          if (weight != Weight.MaxValue) {
            assert(
              size > revision.desiredCubeSize * 0.9,
              s"cube ${cubeId.string} appear as overflowed but has size $size")

            cubeId.parent match {
              case None => // cube is root
              case Some(parent) =>
                assert(
                  cubeSizes(parent) > revision.desiredCubeSize * 0.9,
                  s"cube ${cubeId.string} is overflowed but parent ${parent.string} is not")
            }
          }
        case None =>
      }
    }
  }

  def checkCubes(weightMap: Map[CubeId, Weight]): Unit = {

    weightMap.foreach { case (cube, _) =>
      cube.parent.foreach { parent =>
        assert(
          weightMap.contains(parent),
          s"parent ${parent.string} of ${cube.string} does not appear in the list of cubes")
      }
    }
  }

  def checkCubesOnData(
      weightMap: Map[CubeId, Weight],
      indexed: DataFrame,
      dimensionCount: Int): Unit = {

    indexed
      .select(cubeColumnName)
      .distinct()
      .collect()
      .foreach(row => {
        val cube = CubeId(dimensionCount, row.getAs[Array[Byte]](cubeColumnName))
        assert(weightMap.contains(cube) || weightMap.contains(cube.parent.get))
      })
  }

  def checkWeightsIncrement(weightMap: Map[CubeId, Weight]): Unit = {

    weightMap.foreach { case (cube: CubeId, maxWeight: Weight) =>
      val children = cube.children.toSet
      val childrenWeights = weightMap.filter { case (candidate, _) =>
        children.contains(candidate)
      }
      childrenWeights.foreach { case (child, childWeight) =>
        assert(
          childWeight >= maxWeight,
          s"assertion failed" +
            s" max weight of son ${child.string} is ${childWeight.fraction} " +
            s"and max weight of parent ${cube.string} is ${maxWeight.fraction}")
      }
    }
  }

  def createDF(): DataFrame = {
    val spark = SparkSession.active

    val rdd =
      spark.sparkContext.parallelize(
        0.to(100000)
          .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))

    spark.createDataFrame(rdd)
  }

  "Indexing method" should "respect the desired cube size" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "age,val2"))

        val (indexed, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubeSize(tc, rev, indexed)
      }
    }
  }

  it should "not miss any cube" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "age,val2"))

        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkCubes(tc.indexChanges.cubeWeights)
      }
    }
  }

  it should "respect the weight of the fathers" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "age,val2"))

        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        checkWeightsIncrement(tc.indexChanges.cubeWeights)
      }
    }
  }

  it should "add only leaves to indexed data" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "age,val2"))

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

        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "user_id,product_id"))
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
        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "age,val2"))

        val (i, _) = oTreeAlgorithm.index(df, IndexStatus(rev))
        val firstIndexed = i.withColumnRenamed(cubeColumnName, cubeColumnName + "First")

        df.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", "age,val2")
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

        val offset = 0.5
        val appendData = df
          .withColumn("age", (col("age") * offset).cast(IntegerType))
          .withColumn("val2", (col("val2") * offset).cast(LongType))

        val appendRev = SparkRevisionBuilder.createNewRevision(
          tableId,
          appendData,
          Map("columnsToIndex" -> "age,val2"))
        val (indexed, tc) =
          oTreeAlgorithm.index(appendData, qbeastSnapshot.loadIndexStatus)

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
          val rev = SparkRevisionBuilder.createNewRevision(
            QTableID("test"),
            df,
            Map("columnsToIndex" -> "age,val2"))
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
        val rev = SparkRevisionBuilder.createNewRevision(
          QTableID("test"),
          df,
          Map("columnsToIndex" -> "age,val2"))
        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))

        df.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", "age,val2")
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)

        deltaLog.snapshot.allFiles.collect() foreach (f =>
          {
            val cubeId = CubeId(2, f.tags("cube"))
            cubeId.parent match {
              case None => // cube is root
              case Some(parent) =>
                val minWeight = Weight(f.tags("minWeight").toInt)
                val parentMaxWeight = tc.indexChanges.cubeWeights(parent)

                minWeight should be >= parentMaxWeight
            }
          }: Unit)
      }
    }

}
