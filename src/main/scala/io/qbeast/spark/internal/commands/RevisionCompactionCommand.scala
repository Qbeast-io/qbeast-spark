/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model._
import io.qbeast.spark.delta.writer.SparkDeltaDataWriter
import io.qbeast.spark.delta.{CubeDataLoader, DeltaQbeastSnapshot, SparkDeltaMetadataManager}
import io.qbeast.spark.index.SparkOTreeManager
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Revisions are split into different tiers, depending on the bracket of elementCount they fall in:
 * maxTierElementCount = math.pow(tierScale, tier).toLong * desiredCubeSize.
 * Tier capacity is defined as maxTierElementCount * maxTierRevisionCount.
 * When the sum of elementCounts exceed this value, or the number of revisions
 * exceed maxTierRevisionCount, the tier is to be compacted.
 * When several consecutive tiers are all full, or the compaction of one tier triggers
 * the compaction of one or more tiers down the line, all involved revisions are compacted
 * in one operation.
 * @param tableID path to the table to compact
 * @param tierScale determines the tier capacities and how they from tier to tier
 * @param maxTierRevisionCount the max number of revisions to have for each tier
 */
case class RevisionCompactionCommand(
    tableID: QTableID,
    tierScale: Int = 10,
    maxTierRevisionCount: Int = 10)
    extends LeafRunnableCommand {

  private val indexManager: IndexManager[DataFrame] = SparkOTreeManager
  private val metadataManager: MetadataManager[StructType, FileAction] = SparkDeltaMetadataManager
  private val dataWriter: DataWriter[DataFrame, StructType, FileAction] = SparkDeltaDataWriter

  override def run(spark: SparkSession): Seq[Row] = {
    val deltaLog = DeltaLog.forTable(spark, tableID.id)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val revisionGroups = findRevisionsToCompact(qbeastSnapshot)
    for (revisionGroup <- revisionGroups) {
      executeLeveledCompaction(spark, revisionGroup)
    }
    Seq.empty
  }

  def tierCapacity(tier: Int, desiredCubeSize: Int): Long = {
    // tier 0: tierScale ^ 0 * desiredCubeSize * maxTierRevisionCount
    // tier 1: tierScale ^ 1 * desiredCubeSize * maxTierRevisionCount
    // tier 2: tierScale ^ 2 * desiredCubeSize * maxTierRevisionCount
    // ...
    val maxTierElementCount = math.pow(tierScale, tier).toLong * desiredCubeSize
    maxTierElementCount * maxTierRevisionCount
  }

  def revisionTier(value: Double, base: Int = tierScale): Int = {
    val tier = math.ceil(math.log10(value) / math.log10(base)).toInt
    math.max(0, tier)
  }

  /**
   * Extract elementCount for each Revision and group them by their tier
   */
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
      val tc = tierCapacity(tier, cubeSize)
      val revTier = revisionTiers(tier)
      val tierRowCount = revTier.elementCount
      val numRevisions =
        if (accRowCount == 0) revTier.revisions.size else revTier.revisions.size + 1
      accRowCount + tierRowCount >= tc || numRevisions >= maxTierRevisionCount
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

  /**
   * Executing the compaction of a single RevisionGroup. All records are indexed at once, and
   * the latest revision is used for the compacted data, other revisions are removed from the
   * table metadata.
   */
  def executeLeveledCompaction(spark: SparkSession, revisionGroup: RevisionGroup): Unit = {
    val allAddFiles = CubeDataLoader(tableID).loadFilesFromRevisions(revisionGroup.revisions)
    val paths = allAddFiles.map(a => new Path(tableID.id, a.path).toString)
    val data = spark.read.parquet(paths: _*)

    // Revisions are compacted to the latest one:
    // e.g. rev1 + rev2 + rev3 -> rev3
    // Both rev1 and rev2 will be removed from the table metadata
    val finalRevision = revisionGroup.revisions.maxBy(_.timestamp)
    val indexStatus = IndexStatus(finalRevision)
    val compactedRevisionIDs =
      revisionGroup.revisions.map(_.revisionID).filter(_ != finalRevision.revisionID)
    val removeFiles = allAddFiles.map(_.remove)
    val schema = data.schema
    metadataManager.updateWithTransaction(tableID, schema, append = true) {
      val (qbeastData, tableChanges) = indexManager.index(data, indexStatus)
      val fileActions = dataWriter.write(tableID, schema, qbeastData, tableChanges)

      val revCompactionTc = RevisionCompactionTableChanges(tableChanges, compactedRevisionIDs)
      (revCompactionTc, fileActions ++ removeFiles)
    }
  }

}

case class RevisionGroup(revisions: Seq[Revision], elementCount: Long)
