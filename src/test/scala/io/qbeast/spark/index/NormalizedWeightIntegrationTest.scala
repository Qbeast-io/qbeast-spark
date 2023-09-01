package io.qbeast.spark.index

import io.qbeast.TestClasses.Client3
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{Dataset, SparkSession}
import io.qbeast.spark.utils.State

class NormalizedWeightIntegrationTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143))
      .toDF()
      .as[Client3]
  }

  "CubeNormalizedWeights" should
    "write a the right Weight with a single full file" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        val cubeSize = 10000
        val df = createDF(cubeSize).repartition(1)
        val names = List("age", "val2")
        // val dimensionCount = names.length
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        spark.read.format("qbeast").load(tmpDir).count() shouldBe cubeSize

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val snapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
        val index = snapshot.loadLatestIndexStatus
        index.cubesStatuses.size shouldBe 1
        val status = index.cubesStatuses.values.head
        status.files.size shouldBe 1
        val block = status.files.head
        block.state shouldBe State.FLOODED
    }

  it should
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
}
