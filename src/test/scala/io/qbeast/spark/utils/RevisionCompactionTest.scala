/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.QTableID
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.delta.writer.SparkDeltaDataWriter
import io.qbeast.spark.delta.{DeltaQbeastSnapshot, SparkDeltaMetadataManager}
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.internal.commands.RevisionCompactionCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

class RevisionCompactionTest extends QbeastIntegrationTestSpec {
  val dcs = 500
  val tierScale = 3
  val maxTierSize = 3
  // tier 0: maxRevisionSize: 500, tierCapacity: 1500
  // tier 1: maxRevisionSize: 1500, tierCapacity: 4500
  // tier 2: maxRevisionSize: 4500, tierCapacity: 13500
  // tier 3: maxRevisionSize: 13500, tierCapacity: 40500
  // tier 4: maxRevisionSize: 40500, tierCapacity: 121500
  // ...

  object Compactor {

    def apply(path: String): RevisionCompactionCommand = {
      RevisionCompactionCommand(
        new QTableID(path),
        tierScale,
        maxTierSize,
        SparkOTreeManager,
        SparkDeltaMetadataManager,
        SparkDeltaDataWriter)
    }

  }

  def createDataFromTo(f: Int, t: Int, path: String, spark: SparkSession): Unit = {
    import spark.implicits._

    (f until t)
      .map(i => T2(i, i))
      .toDF()
      .write
      .mode("append")
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", dcs)
      .save(path)
  }

  "revisionTier" should "classify revisions correctly" in withSparkAndTmpDir((_, tmpDir) => {
    val compactor = Compactor(tmpDir)
    Seq((1, 0), (500, 0), (1500, 1), (13500, 3), (88573500, 11)).foreach {
      case (elemCount, tier) =>
        compactor.revisionTier(elemCount.toDouble / dcs) shouldBe tier
    }
  })

  "computeRevisionTiers" should "divide revisions in tiers" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val valueRanges = Seq(
        (0, 500), // 500
        (500, 1000), // 500
        (1000, 2000), // 1000
        (2000, 4000), // 2000
        (4000, 24000) // 20000
      )
      // tier 0: maxRevisionSize: 500, tierCapacity: 1500
      // revision row counts: 500, 500, sum: 1000

      // tier 1: maxRevisionSize: 1500, tierCapacity: 4500
      // revision row counts: 1000, sum: 1000

      // tier 2: maxRevisionSize: 4500, tierCapacity: 13500
      // revision row counts: 2000, 2000, sum: 2000

      // tier 4: maxRevisionSize: 40500, tierCapacity: 121500
      // revision row counts: 20000 sum: 20000

      valueRanges.foreach { case (f, t) =>
        createDataFromTo(f, t, tmpDir, spark)
      }

      val revisions =
        DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).snapshot).loadAllRevisions
      val compactor = Compactor(tmpDir)
      val (revisionTiers, _) =
        compactor.computeRevisionTiersAndRowCounts(revisions, dcs)
      revisionTiers.keys.toSeq.sorted shouldBe Seq(0, 1, 2, 4)
      revisionTiers(0).elementCount shouldBe 1000
      revisionTiers(1).elementCount shouldBe 1000
      revisionTiers(2).elementCount shouldBe 2000
      revisionTiers(4).elementCount shouldBe 20000
    })

  "findRevisionsToCompact" should "group revisions for compaction" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val valueRanges = Seq(
        (0, 500), // 500
        (500, 1000), // 500
        (1000, 1500), // 500
        (1500, 3000), // 1500
        (3000, 4500), // 1500
        (4500, 18000), // 13500
        (18000, 31500), // 13500
        (31500, 63000) // 13500
      )
      // tier 0: maxRevisionSize: 500, tierCapacity: 1500
      // revision row count: 500, 500, 500, sum: 1500

      // tier 1: maxRevisionSize: 1500, tierCapacity: 4500
      // revision row count: 1500, 1500, sum: 3000

      // tier 2: maxRevisionSize: 4500, tierCapacity: 13500

      // tier 3: maxRevisionSize: 13500, tierCapacity: 40500
      // revision row count: 13500, 13500, 13500, sum: 40500

      valueRanges.foreach { case (f, t) =>
        createDataFromTo(f, t, tmpDir, spark)
      }

      val qSnapshot = DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).snapshot)
      val compactor = Compactor(tmpDir)
      val revisionsToCompact = compactor.findRevisionsToCompact(qSnapshot)

      revisionsToCompact.size shouldBe 2
      revisionsToCompact.head.revisions.map(_.revisionID).sorted shouldBe Seq(1, 2, 3, 4, 5)
      revisionsToCompact(1).revisions.map(_.revisionID).sorted shouldBe Seq(6, 7, 8)
    })

  "executeRevisionCompaction" should "maintain the number of elements" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val valueRanges =
        Seq(
          (0, 500), // 500
          (500, 1000), // 500
          (1000, 1500), // 500
          (1500, 3000), // 1500
          (3000, 4500), // 1500
          (4500, 6001) // 1501
        )
      // tier 0: maxRevisionSize: 500, tierCapacity: 1500
      // revision row count: 500, 500, 500, sum: 1500

      // tier 1: maxRevisionSize: 1500, tierCapacity: 4500
      // revision row count: 1500, 1500, sum: 3000

      // tier 2: maxRevisionSize: 4500, tierCapacity: 13500
      // revision row count: 1501, sum: 1501

      valueRanges.foreach { case (f, t) =>
        createDataFromTo(f, t, tmpDir, spark)
      }

      val compactor = Compactor(tmpDir)
      val rowCountBefore = spark.read.format("qbeast").load(tmpDir).count()

      compactor.run(spark)

      val rowCountAfter = spark.read.format("qbeast").load(tmpDir).count()
      rowCountAfter shouldBe rowCountBefore

      val qbeastSnapshot = DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).snapshot)
      val allRevisionIDs = qbeastSnapshot.loadAllRevisions.map(_.revisionID)
      val revisionIDsAfter =
        allRevisionIDs
          .filterNot(id => qbeastSnapshot.loadRevisionBlocks(id).isEmpty)

      revisionIDsAfter.size shouldBe 2
      revisionIDsAfter.contains(5) shouldBe true
      revisionIDsAfter.contains(6) shouldBe true
    })
}
