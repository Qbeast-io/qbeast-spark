package io.qbeast.spark.delta

import io.qbeast.core.model.{CubeId, CubeStatus, Weight}
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.delta.DeltaLog

class IndexStatusBuilderTest extends QbeastIntegrationTestSpec {

  "IndexBuilder" should "build cube information from DeltaLog" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val data = 0.to(100000).toDF("id")

      // Append data x times
      data.write
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("cubeSize", "10000")
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val indexStatus =
        DeltaQbeastSnapshot(deltaLog.snapshot).loadLatestIndexStatus

      val blockSizes = indexStatus.cubesStatuses.mapValues(_.files.size).values.toSet
      blockSizes.contains(1) shouldBe true
      blockSizes.contains(2) shouldBe true

      indexStatus.revision.revisionID shouldBe 1
      indexStatus.replicatedSet shouldBe Set.empty
      indexStatus.announcedSet shouldBe Set.empty
    })

  it should "work well on appending the same revision" in withSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val data = 0.to(100000).toDF("id")

    // Append data x times
    data.write
      .format("qbeast")
      .option("columnsToIndex", "id")
      .option("cubeSize", "10000")
      .save(tmpDir)
    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val firstIndexStatus = DeltaQbeastSnapshot(deltaLog.snapshot).loadLatestIndexStatus

    data.write
      .format("qbeast")
      .mode("append")
      .option("columnsToIndex", "id")
      .option("cubeSize", "10000")
      .save(tmpDir)
    val secondIndexStatus = DeltaQbeastSnapshot(deltaLog.update()).loadLatestIndexStatus

    val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
    val blockCounts = metrics.cubeStatuses.mapValues(_.files.size).values.toSet
    blockCounts.contains(1) shouldBe true
    blockCounts.contains(2) shouldBe true
    blockCounts.contains(3) shouldBe true

    secondIndexStatus.revision.revisionID shouldBe 1
    secondIndexStatus.announcedSet shouldBe Set.empty
    secondIndexStatus.replicatedSet shouldBe Set.empty
    secondIndexStatus.cubesStatuses.foreach { case (cube: CubeId, cubeStatus: CubeStatus) =>
      if (cubeStatus.maxWeight < Weight.MaxValue) {
        firstIndexStatus.cubesStatuses.get(cube) shouldBe defined
        cubeStatus.maxWeight shouldBe cubeStatus.files.map(_.maxWeight).min
        cubeStatus.maxWeight shouldBe <=(firstIndexStatus.cubesStatuses(cube).maxWeight)
      }
    }
  })

}
