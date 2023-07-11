/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.TestClasses.Client3
import io.qbeast.core.model.CubeId
import io.qbeast.spark.{QbeastIntegrationTestSpec, delta}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionID
import io.qbeast.spark.table.IndexedTable

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
      val revisionId = getLatestRevisionId(spark, tmpDir)
      val announcedCubes = getIndexedTable(tmpDir).analyze(revisionId)
      announcedCubes shouldBe Seq(CubeId.root(dimensionCount).string)
  }

  it should "not analyze replicated cubes" in withSparkAndTmpDir { (spark, tmpDir) =>
    appendNewRevision(spark, tmpDir, 1)
    val revisionId = getLatestRevisionId(spark, tmpDir)
    val table = getIndexedTable(tmpDir)
    table.replicate(revisionId)
    table.replicate(revisionId)

    val snapshot = delta.DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).snapshot)
    val index = snapshot.loadLatestIndexStatus
    val replicatedCubes = index.replicatedSet

    val announcedCubes = table.analyze(revisionId)
    announcedCubes.foreach(a => replicatedCubes shouldNot contain(a))
  }

  "Optimize command" should "replicate cubes in announce set" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      appendNewRevision(spark, tmpDir, 1)
      val revisionId = getLatestRevisionId(spark, tmpDir)

      val table = getIndexedTable(tmpDir)

      (0 to 5).foreach(_ => {
        val announcedCubes = table.analyze(revisionId)
        table.replicate(revisionId)
        val snapshot = delta.DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).snapshot)
        val replicatedCubes = snapshot.loadLatestIndexStatus.replicatedSet.map(_.string)
        announcedCubes.foreach(r => replicatedCubes should contain(r))
      })
  }

  private def getLatestRevisionId(spark: SparkSession, path: String): RevisionID = {
    val snapshot = delta.DeltaQbeastSnapshot(DeltaLog.forTable(spark, path).snapshot)
    snapshot.loadLatestRevision.revisionID
  }

  private def getIndexedTable(path: String): IndexedTable = {
    QbeastContext.indexedTableFactory.getIndexedTable(QTableID(path))
  }

}
