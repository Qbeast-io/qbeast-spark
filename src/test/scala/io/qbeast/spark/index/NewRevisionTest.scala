/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.OTreeAlgorithmTest.Client3
import io.qbeast.spark.sql.qbeast.QbeastSnapshot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      .option("columnsToIndex", names.mkString(","))
      .save(tmpDir)
  }

  "new revision" should "create different revisions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val spaceMultipliers = List(1, 3, 8)
      spaceMultipliers.foreach(i => appendNewRevision(spark, tmpDir, i))

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, 10000)

      qbeastSnapshot.spaceRevisions.size shouldBe spaceMultipliers.length

  }

  it should
    "create different index structure for each one" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        appendNewRevision(spark, tmpDir, 1)
        appendNewRevision(spark, tmpDir, 3)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = QbeastSnapshot(deltaLog.snapshot, 10000)

        val allWM = qbeastSnapshot.spaceRevisions.map(_.timestamp).map(qbeastSnapshot.cubeWeights)
        allWM.foreach(wm => assert(wm.nonEmpty))
    }

}
