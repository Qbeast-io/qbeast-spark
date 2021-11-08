/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import com.typesafe.config.ConfigFactory
import io.qbeast.model.{IndexStatus, QTableID, Weight}
import io.qbeast.spark.index.OTreeAlgorithmTest.Client3
import io.qbeast.spark.{QbeastIntegrationTestSpec, SparkRevisionBuilder, delta}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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
          val df = createDF(10000 / 2).repartition(1)
          val names = List("age", "val2")
          // val dimensionCount = names.length
          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", names.mkString(","))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot)
          val cubeNormalizedWeights =
            qbeastSnapshot.lastRevisionData.cubeNormalizedWeights

          cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe 2.0)
        }

    }

  it should "normalize weights when cubes are full" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      withOTreeAlgorithm { _ =>
        val df = createDF(ConfigFactory.load().getInt("qbeast.index.size")).repartition(1)
        val names = List("age", "val2")

        df.write
          .format("qbeast")
          .mode("overwrite")
          .option("columnsToIndex", names.mkString(","))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.QbeastSnapshot(deltaLog.snapshot)
        val cubeNormalizedWeights =
          qbeastSnapshot.lastRevisionData.cubeNormalizedWeights

        cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe <(1.0))
      }

  }

  "CubeWeights" should
    "reflect the estimation through the Delta Commit Log" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          val df = createDF(100000)
          val indexStatus = IndexStatus(
            SparkRevisionBuilder
              .createNewRevision(QTableID("test"), df, Map("columnsToIndex" -> "age,val2")))
          val (_, tc) = oTreeAlgorithm.index(df, indexStatus)
          val weightMap = tc.indexChanges.cubeWeights
          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", "age,val2")
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = delta.QbeastSnapshot(deltaLog.snapshot)
          val commitLogWeightMap = qbeastSnapshot.lastRevisionData.cubeWeights

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
      val qbeastSnapshot = delta.QbeastSnapshot(deltaLog.snapshot)
      val cubeWeights = qbeastSnapshot.lastRevisionData.cubeWeights

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
            val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot)
            val isBuilder =
              new qbeastSnapshot.DeltaSnapshotIndexStatusBuilder(qbeastSnapshot.lastRevision)

            val indexStateMethod = PrivateMethod[Dataset[CubeInfo]]('revisionState)
            val indexState =
              isBuilder invokePrivate indexStateMethod()
            val overflowed =
              qbeastSnapshot.lastRevisionData.overflowedSet.map(_.string)

            indexState
              .filter(cubeInfo => overflowed.contains(cubeInfo.cube))
              .collect()
              .foreach(cubeInfo =>
                assert(
                  cubeInfo.size > 1000 * 0.9,
                  "assertion failed in cube " + cubeInfo.cube +
                    " where size is " + cubeInfo.size + " and weight is " + cubeInfo.maxWeight))
          }
        }
    }

}
