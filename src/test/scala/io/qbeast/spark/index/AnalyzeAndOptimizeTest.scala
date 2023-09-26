/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.TestClasses.Client3
import io.qbeast.core.model.CubeId
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable, delta}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnalyzeAndOptimizeTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {

  def appendNewRevision(spark: SparkSession, tmpDir: String, multiplier: Int): Int = {

    val rdd =
      spark.sparkContext.parallelize(0
        .to(100000)
        .flatMap(i =>
          Seq(
            Client3(i, s"student-${i}", i, (i + 123) * multiplier, i * 4),
            Client3(i * i, s"student-${i}", i, (i * 1000 + 123) * multiplier, i * 2567.3432143))))

    val df = spark.createDataFrame(rdd)
    val names = List("age", "val2")
    df.write
      .format("qbeast")
      .mode("append")
      .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
      .save(tmpDir)
    names.length
  }

  "Analyze command" should "announce root when the tree is not replicated" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val dimensionCount = appendNewRevision(spark, tmpDir, 1)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      val announcedCubes = qbeastTable.analyze()
      announcedCubes shouldBe Seq(CubeId.root(dimensionCount).string)
  }

  it should "not analyze replicated cubes" in withSparkAndTmpDir { (spark, tmpDir) =>
    appendNewRevision(spark, tmpDir, 1)
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    qbeastTable.analyze()
    qbeastTable.optimize()
    qbeastTable.analyze()
    qbeastTable.optimize()

    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.update())
    val replicatedCubes = qbeastSnapshot.loadLatestIndexStatus.replicatedSet

    val announcedCubes = qbeastTable.analyze()
    announcedCubes.foreach(a => replicatedCubes shouldNot contain(a))
  }

  "Optimize command" should "replicate cubes in announce set" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      appendNewRevision(spark, tmpDir, 1)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      (0 to 5).foreach(_ => {
        val announcedCubes = qbeastTable.analyze()
        qbeastTable.optimize()
        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.update())
        val replicatedCubes =
          qbeastSnapshot.loadLatestIndexStatus.replicatedSet.map(_.string)

        announcedCubes.foreach(r => replicatedCubes should contain(r))
      })
  }
}
