/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model._
import io.qbeast.spark.delta.{CubeDataLoader, DeltaQbeastSnapshot}
import io.qbeast.spark.index.SparkOTreeManager
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class RevisionCompactionCommand(
    tableID: QTableID,
    tierScale: Int = 10,
    maxTierSize: Int = 10,
    indexManager: IndexManager[DataFrame],
    metadataManager: MetadataManager[StructType, FileAction],
    dataWriter: DataWriter[DataFrame, StructType, FileAction])
    extends LeafRunnableCommand {

  // Revisions are split into different tiers, depending on their number of elements
  // tier 0: tierScale ^ 0 * desiredCubeSize
  // tier 1: tierScale ^ 1 * desiredCubeSize
  // tier 2: tierScale ^ 2 * desiredCubeSize

  // There can be a number of maxTierSize revisions per tier, and
  // maxTierSize * tierCapacity records in each tier. Otherwise the tier
  // should be compacted.

  override def run(spark: SparkSession): Seq[Row] = {
    val deltaLog = DeltaLog.forTable(spark, tableID.id)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val revisionGroups = findRevisionsToCompact(qbeastSnapshot)
    for (revisionGroup <- revisionGroups) {
      executeLeveledCompaction(spark, revisionGroup)
    }
    Seq.empty
  }

  def revisionTier(value: Double, base: Int = tierScale): Int = {
    val tier = math.ceil(math.log10(value) / math.log10(base)).toInt
    math.max(0, tier)
  }

  def computeRevisionTiersAndRowCounts(
      revisions: Seq[Revision],
      cubeSize: Int): (Map[Int, RevisionGroup], Map[RevisionID, Long]) = {
    val revisionRowCounts = CubeDataLoader(tableID).revisionRowCount()
    val revisionTiers = revisions
      .filter(rev => revisionRowCounts.contains(rev.revisionID))
      .groupBy(rev => {
        val elementCount = revisionRowCounts(rev.revisionID).toDouble
        revisionTier(elementCount / cubeSize)
      })
      .map { case (tier, revisions) =>
        (
          tier,
          RevisionGroup(revisions, revisions.map(rev => revisionRowCounts(rev.revisionID)).sum))
      }

    (revisionTiers, revisionRowCounts)
  }

  def includeTier(revisionTiers: Map[Int, RevisionGroup], cubeSize: Int)(
      accRowCount: Long,
      tier: Int): Boolean = {
    if (revisionTiers.contains(tier)) {
      // tierCapacity = maxRevisionSize * numRevisionsPerTier
      val tierCapacity = math.pow(tierScale, tier).toLong * cubeSize * maxTierSize
      val revTier = revisionTiers(tier)
      val tierRowCount = revTier.elementCount
      val numRevisions =
        if (accRowCount == 0) revTier.revisions.size else revTier.revisions.size + 1
      accRowCount + tierRowCount >= tierCapacity || numRevisions >= maxTierSize
    } else false
  }

  def findRevisionsToCompact(qbeastSnapshot: DeltaQbeastSnapshot): Seq[RevisionGroup] = {
    val revisions = qbeastSnapshot.loadAllRevisions
    if (revisions.isEmpty) Nil
    else {
      val cubeSize = revisions.head.desiredCubeSize
      val (revisionTiers, revisionRowCount) =
        computeRevisionTiersAndRowCounts(revisions, cubeSize)
      def includeCurrTier: (Long, Int) => Boolean = includeTier(revisionTiers, cubeSize)

      var tier = revisionTiers.keys.min
      val maxTier = revisionTiers.keys.max
      val revisionsToCompact = Seq.newBuilder[RevisionGroup]
      revisionsToCompact.sizeHint(maxTier - tier)

      while (tier <= maxTier) {
        if (revisionTiers.contains(tier)) {
          var accRowCount = 0L
          val revisionGroup = Seq.newBuilder[Revision]
          while (includeCurrTier(accRowCount, tier)) {
            val revisionTier = revisionTiers(tier)
            accRowCount += revisionTier.elementCount
            revisionGroup ++= revisionTier.revisions
            tier += 1
          }
          val revisions = revisionGroup.result()
          if (revisions.nonEmpty) {
            revisionsToCompact += RevisionGroup(
              revisions,
              revisions.map(rev => revisionRowCount(rev.revisionID)).sum)
          }
        }
        tier += 1
      }

      revisionsToCompact.result()
    }
  }

  def executeLeveledCompaction(spark: SparkSession, revisionGroup: RevisionGroup): Unit = {
    val allAddFiles = CubeDataLoader(tableID).loadFilesFromRevisions(revisionGroup.revisions)
    val paths = allAddFiles.map(a => new Path(tableID.id, a.path).toString)
    val data = spark.read.parquet(paths: _*)

    // Revision and IndexStatus to use
    val finalRevision = revisionGroup.revisions.maxBy(_.timestamp)
    val indexStatus = IndexStatus(finalRevision)

    // Revisions to remove
    val compactedRevisionIDs =
      revisionGroup.revisions.map(_.revisionID).filter(_ != finalRevision.revisionID)
    val removeFiles = allAddFiles.map(_.remove)
    val schema = data.schema
    metadataManager.updateWithTransaction(tableID, schema, append = true) {
      val (qbeastData, tableChanges) = SparkOTreeManager.index(data, indexStatus)
      val fileActions = dataWriter.write(tableID, schema, qbeastData, tableChanges)

      val revCompactionTc = RevisionCompactionTableChanges(tableChanges, compactedRevisionIDs)
      (revCompactionTc, fileActions ++ removeFiles)
    }
  }

}

case class RevisionGroup(revisions: Seq[Revision], elementCount: Long)
