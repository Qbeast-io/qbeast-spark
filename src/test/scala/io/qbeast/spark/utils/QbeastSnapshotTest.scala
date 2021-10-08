/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.OTreeAlgorithmTest.Client3
import io.qbeast.spark.index.{Weight}
import io.qbeast.spark.model.CubeInfo
import io.qbeast.spark.sql.qbeast.QbeastSnapshot
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastSnapshotTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {

  def createDF(size: Int): DataFrame = {
    val spark = SparkSession.active

    val rdd =
      spark.sparkContext.parallelize(
        1.to(size)
          .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))

    assert(rdd.count() == size)
    spark.createDataFrame(rdd)
  }

  "CubeNormalizedWeights" should
    "normalize weights when cubes are half full" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          val df = createDF(oTreeAlgorithm.desiredCubeSize / 2).repartition(1)
          val names = List("age", "val2")
          // val dimensionCount = names.length
          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", names.mkString(","))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize)
          val cubeNormalizedWeights =
            qbeastSnapshot.cubeNormalizedWeights(qbeastSnapshot.lastRevisionTimestamp)

          cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe 2.0)
        }

    }

  it should "normalize weights when cubes are full" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF(oTreeAlgorithm.desiredCubeSize).repartition(1)
        val names = List("age", "val2")
        // val dimensionCount = names.length
        df.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", names.mkString(","))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize)
        val cubeNormalizedWeights =
          qbeastSnapshot.cubeNormalizedWeights(qbeastSnapshot.lastRevisionTimestamp)

        cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe <(1.0))
      }

  }

  "CubeWeights" should
    "reflect the estimation through the Delta Commit Log" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          val df = createDF(100000)
          val names = List("age", "val2")
          val (_, _, weightMap) = oTreeAlgorithm.indexFirst(df, names)

          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", names.mkString(","))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize)
          val commitLogWeightMap =
            qbeastSnapshot.cubeWeights(qbeastSnapshot.lastRevisionTimestamp)

          // commitLogWeightMap shouldBe weightMap
          commitLogWeightMap.keys.foreach(cubeId => {
            assert(weightMap.contains(cubeId) || weightMap.contains(cubeId.parent.get))
          })
        }

    }

  it should "respect the (0.0, 1.0] range" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      val df = createDF(100000)
      val names = List("age", "val2")

      df.write
        .format("qbeast")
        .mode("overwrite")
        .option("columnsToIndex", names.mkString(","))
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize)
      val cubeWeights = qbeastSnapshot.cubeWeights(qbeastSnapshot.lastRevisionTimestamp)

      cubeWeights.foreach { case (_, weight: Weight) =>
        weight shouldBe >(Weight.MinValue)
        weight shouldBe <=(Weight.MaxValue)
      }
    }

  }

  "Overflowed set" should
    "contain only cubes that surpass desiredCubeSize" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          {

            val df = createDF(100000)
            val names = List("age", "val2")

            df.write
              .format("qbeast")
              .mode("overwrite")
              .option("columnsToIndex", names.mkString(","))
              .save(tmpDir)

            val deltaLog = DeltaLog.forTable(spark, tmpDir)
            val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, oTreeAlgorithm.desiredCubeSize)

            val indexStateMethod = PrivateMethod[Dataset[CubeInfo]]('indexState)
            val indexState =
              qbeastSnapshot invokePrivate indexStateMethod(qbeastSnapshot.lastRevisionTimestamp)
            val overflowed =
              qbeastSnapshot
                .overflowedSet(qbeastSnapshot.lastRevisionTimestamp)
                .map(_.string)

            indexState
              .filter(cubeInfo => overflowed.contains(cubeInfo.cube))
              .collect()
              .foreach(cubeInfo =>
                assert(
                  cubeInfo.size > oTreeAlgorithm.desiredCubeSize * 0.9,
                  "assertion failed in cube " + cubeInfo.cube +
                    " where size is " + cubeInfo.size + " and weight is " + cubeInfo.maxWeight))
          }
        }
    }

}
