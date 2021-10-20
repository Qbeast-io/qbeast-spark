/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.OTreeAlgorithmTest.{Client3, Client4}
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import io.qbeast.spark.sql.qbeast.QbeastSnapshot
import org.apache.spark.SparkException
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  def checkCubeSize(
      weightMap: Map[CubeId, Weight],
      indexed: DataFrame,
      dimensionCount: Int,
      desiredSize: Int): Unit = {

    val cubeSizes = indexed
      .groupBy(cubeColumnName)
      .count()
      .collect()
      .map(row =>
        (
          CubeId(dimensionCount, row.getAs[Array[Byte]](cubeColumnName)),
          row.getAs[Long]("count")))
      .toMap

    cubeSizes.foreach { case (cubeId: CubeId, size: Long) =>
      weightMap.get(cubeId) match {
        case Some(weight) =>
          if (weight != Weight.MaxValue) {
            assert(
              size > desiredSize * 0.9,
              s"cube ${cubeId.string} appear as overflowed but has size $size")

            cubeId.parent match {
              case None => // cube is root
              case Some(parent) =>
                assert(
                  cubeSizes(parent) > desiredSize * 0.9,
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
        val names = List("age", "val2")
        val (indexed, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)

        checkCubeSize(weightMap, indexed, names.length, oTreeAlgorithm.desiredCubeSize)
      }
    }
  }

  it should "not miss any cube" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val names = List("age", "val2")
        val (_, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)

        checkCubes(weightMap)
      }
    }
  }

  it should "respect the weight of the fathers" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val names = List("age", "val2")
        val (_, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)

        checkWeightsIncrement(weightMap)
      }
    }
  }

  it should "add only leaves to indexed data" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF()
        val names = List("age", "val2")
        val (indexed, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)

        checkCubesOnData(weightMap, indexed, names.length)
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

        val names = List("user_id", "product_id")
        val (indexed, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)

        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, names.length)
        checkCubeSize(weightMap, indexed, names.length, oTreeAlgorithm.desiredCubeSize)

      }
    }
  }

  it should "maintain correctness on append" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF()
        val names = List("age", "val2")
        val (i, _, _) = oTreeAlgorithm.indexFirst(df, names)
        val firstIndexed = i.withColumnRenamed(cubeColumnName, cubeColumnName + "First")

        df.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", names.mkString(","))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize)

        val offset = 0.5
        val appendData = df
          .withColumn("age", col("age") * offset)
          .withColumn("val2", col("val2") * offset)
        val (indexed, _, weightMap) =
          oTreeAlgorithm.indexNext(appendData, qbeastSnapshot, Set.empty)

        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, names.length)
        checkCubeSize(
          weightMap,
          indexed.join(
            firstIndexed,
            firstIndexed(cubeColumnName + "First") === indexed(cubeColumnName)),
          names.length,
          oTreeAlgorithm.desiredCubeSize)

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
        val names = List("age", "val2")

        try {
          oTreeAlgorithm.indexFirst(df, names)
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
        val names = List("age", "val2")
        val (_, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)
        val dimensionCount = names.length

        df.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", names.mkString(","))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)

        deltaLog.snapshot.allFiles.collect() foreach (f =>
          {
            val cubeId = CubeId(dimensionCount, f.tags("cube"))
            cubeId.parent match {
              case None => // cube is root
              case Some(parent) =>
                val minWeight = Weight(f.tags("minWeight").toInt)
                val parentMaxWeight = weightMap(parent)

                minWeight should be >= parentMaxWeight
            }
          }: Unit)
      }
    }

}
