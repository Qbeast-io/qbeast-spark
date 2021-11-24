/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.qbeast.TestClasses._

class NewRevisionTest
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with QbeastIntegrationTestSpec {

  def appendNewRevision(spark: SparkSession, tmpDir: String, multiplier: Int): Unit = {

    val rdd =
      spark.sparkContext.parallelize(
        0.to(100000)
          .map(i =>
            Client3(i * i, s"student-$i", i, (i * 1000 + 123) * multiplier, i * 2567.3432143)))
    val df = spark.createDataFrame(rdd)
    val names = List("age", "val2")
    df.write
      .format("qbeast")
      .mode("append")
      .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10000"))
      .save(tmpDir)
  }

  "new revision" should "create different revisions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val spaceMultipliers = List(1, 3, 8)
      spaceMultipliers.foreach(i => appendNewRevision(spark, tmpDir, i))

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)
      val spaceRevisions = qbeastSnapshot.loadAllRevisions

      spaceRevisions.size shouldBe spaceMultipliers.length

  }

  it should
    "create different index structure for each one" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        appendNewRevision(spark, tmpDir, 1)
        appendNewRevision(spark, tmpDir, 3)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

        val revisions = qbeastSnapshot.loadAllRevisions
        val allWM =
          revisions
            .map(revision =>
              qbeastSnapshot.loadIndexStatus(revision.revisionID).cubeNormalizedWeights)
        allWM.foreach(wm => assert(wm.nonEmpty))
    }

  it should
    "create different revision on different desired cube size" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val rdd =
            spark.sparkContext.parallelize(
              Seq(
                Client3(1, s"student-1", 1, 1000 + 123, 2567.3432143),
                Client3(2, s"student-2", 2, 2 * 1000 + 123, 2 * 2567.3432143)))

          val df = spark.createDataFrame(rdd)

          val names = List("age", "val2")
          val cubeSize = 3000

          df.write
            .format("qbeast")
            .mode("overwrite")
            .options(
              Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = delta.DeltaQbeastSnapshot(deltaLog.snapshot)

          qbeastSnapshot.loadLatestRevision.desiredCubeSize shouldBe cubeSize

        }
    }

}
