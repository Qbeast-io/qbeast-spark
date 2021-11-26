/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.TestClasses.Client3
import io.qbeast.model.{CubeStatus, IndexStatus, QTableID, Weight}
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.utils.TagUtils
import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}

class QbeastSnapshotTest extends QbeastIntegrationTestSpec {

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
        val cubeSize = 10000
        val df = createDF(cubeSize / 2).repartition(1)
        val names = List("age", "val2")
        // val dimensionCount = names.length
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
        val cubeNormalizedWeights =
          qbeastSnapshot.loadLatestIndexStatus.cubeNormalizedWeights

        cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe 2.0)
    }

  it should "normalize weights when cubes are full" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val cubeSize = 10000
      val df =
        createDF(cubeSize).repartition(1)
      val names = List("age", "val2")

      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
      val cubeNormalizedWeights =
        qbeastSnapshot.loadLatestIndexStatus.cubeNormalizedWeights

      cubeNormalizedWeights.foreach(cubeInfo => cubeInfo._2 shouldBe 1.0)
  }

  "CubeWeights" should
    "reflect the estimation through the Delta Commit Log" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          val df = createDF(100000)
          val indexStatus = IndexStatus(
            SparkRevisionFactory
              .createNewRevision(
                QTableID("test"),
                df.schema,
                Map("columnsToIndex" -> "age,val2", "cubeSize" -> "10000")))
          val (_, tc) = oTreeAlgorithm.index(df, indexStatus)
          val weightMap = tc.indexChanges.cubeWeights
          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", "age,val2")
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
          val commitLogWeightMap = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

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
        .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
      val cubeWeights = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

      cubeWeights.values.foreach { case CubeStatus(weight, _, _) =>
        weight shouldBe >(Weight.MinValue)
        weight shouldBe <=(Weight.MaxValue)
      }
    }

  }

  "Overflowed set" should
    "contain only cubes that surpass desiredCubeSize" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {

          val df = createDF(100000)
          val names = List("age", "val2")
          val cubeSize = 10000
          df.write
            .format("qbeast")
            .mode("overwrite")
            .options(
              Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
          val builder =
            new IndexStatusBuilder(qbeastSnapshot, qbeastSnapshot.loadLatestIndexStatus.revision)
          val revisionState = builder.buildCubesStatuses

          val overflowed = qbeastSnapshot.loadLatestIndexStatus.overflowedSet
          val fileInfo = qbeastSnapshot.snapshot.allFiles.collect().map(a => (a.path, a)).toMap

          revisionState
            .filter { case (cube, _) => overflowed.contains(cube) }
            .foreach { case (cube, CubeStatus(weight, _, files)) =>
              val size = files
                .map(fileInfo)
                .map(a => a.tags(TagUtils.elementCount).toLong)
                .sum
              assert(
                size > cubeSize * 0.9,
                "assertion failed in cube " + cube +
                  " where size is " + size + " and weight is " + weight)
            }
        }
    }

}
