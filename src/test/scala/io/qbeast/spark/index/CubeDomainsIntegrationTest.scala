package io.qbeast.spark.index

import io.qbeast.core.model.CubeDomainsBuilder
import io.qbeast.core.model.CubeStatus
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Weight
import io.qbeast.spark.delta
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.PrivateMethodTester

class CubeDomainsIntegrationTest extends QbeastIntegrationTestSpec with PrivateMethodTester {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143))
      .toDF()
      .as[Client3]
  }

  "CubeWeights" should
    "reflect the estimation through the Delta Commit Log" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        withOTreeAlgorithm { oTreeAlgorithm =>
          val df = createDF(100000)
          val names = List("age", "val2")
          val indexStatus = IndexStatus(
            SparkRevisionFactory
              .createNewRevision(
                QTableID("test"),
                df.schema,
                Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000")))
          val (_, tc) = oTreeAlgorithm.index(df.toDF(), indexStatus)
          df.write
            .format("qbeast")
            .mode("overwrite")
            .option("columnsToIndex", "age,val2")
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.update())
          val commitLogWeightMap = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

          // commitLogWeightMap shouldBe weightMap
          commitLogWeightMap.keys.foreach(cubeId => {
            tc.cubeWeight(cubeId) should be('defined)
          })
        }

    }

  it should "respect the (0.0, 1.0] range" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    val df = createDF(100000)
    val names = List("age", "val2")

    df.write
      .format("qbeast")
      .mode("overwrite")
      .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
      .save(tmpDir)

    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.update())
    val cubeWeights = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

    cubeWeights.values.foreach { case CubeStatus(_, weight, _, _) =>
      weight shouldBe >(Weight.MinValue)
      weight shouldBe <=(Weight.MaxValue)
    }
  }

  it should "respect the lower bound for groupCubeSize(1000)" in withSpark { _ =>
    val numElements =
      DEFAULT_CUBE_SIZE * CUBE_WEIGHTS_BUFFER_CAPACITY / CubeDomainsBuilder.minGroupCubeSize
    val numPartitions = 1
    val estimateGroupCubeSize = PrivateMethod[Int]('estimateGroupCubeSize)

    // numElements = 5e11 > 5e8 => groupCubeSize < 1000 => groupCubeSize = 1000
    CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
      DEFAULT_CUBE_SIZE,
      numPartitions,
      numElements * 1000,
      CUBE_WEIGHTS_BUFFER_CAPACITY) shouldBe CubeDomainsBuilder.minGroupCubeSize

    // numElements = 5e8 => groupCubeSize = 1000
    CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
      DEFAULT_CUBE_SIZE,
      numPartitions,
      numElements,
      CUBE_WEIGHTS_BUFFER_CAPACITY) shouldBe CubeDomainsBuilder.minGroupCubeSize

    // numElements = 5e6 < 5e8 => groupCubeSize > 1000
    CubeDomainsBuilder invokePrivate estimateGroupCubeSize(
      DEFAULT_CUBE_SIZE,
      numPartitions,
      numElements / 100,
      CUBE_WEIGHTS_BUFFER_CAPACITY) shouldBe >(CubeDomainsBuilder.minGroupCubeSize)
  }

}
