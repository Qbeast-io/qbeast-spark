/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.{QTableID, RevisionID, StagingUtils}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.commands.{
  AnalyzeTableCommand,
  CompactTableCommand,
  OptimizeTableCommand
}
import io.qbeast.spark.table._
import io.qbeast.spark.utils.MathOps.depthOnBalance
import io.qbeast.spark.utils.{CubeSizeMetrics, IndexMetrics}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{AnalysisExceptionFactory, SparkSession}
import io.qbeast.core.model.CubeId

/**
 * Class for interacting with QbeastTable at a user level
 *
 * @param sparkSession active SparkSession
 * @param tableID      QTableID
 * @param indexedTableFactory configuration of the indexed table
 */
class QbeastTable private (
    sparkSession: SparkSession,
    tableID: QTableID,
    indexedTableFactory: IndexedTableFactory)
    extends Serializable
    with StagingUtils {

  private def deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableID.id)

  private def qbeastSnapshot: DeltaQbeastSnapshot =
    delta.DeltaQbeastSnapshot(deltaLog.snapshot)

  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableID)

  private def latestRevisionAvailable = qbeastSnapshot.loadLatestRevision

  private def latestRevisionAvailableID = latestRevisionAvailable.revisionID

  private def checkRevisionAvailable(revisionID: RevisionID): Unit = {
    if (!qbeastSnapshot.existsRevision(revisionID)) {
      throw AnalysisExceptionFactory.create(
        s"Revision $revisionID does not exists. " +
          s"The latest revision available is $latestRevisionAvailableID")
    }
  }

  /**
   * Optimizes the index by compacting and replicating the data which belong
   * to some index revision. Compaction reduces the number of cube files, and
   * replication makes the cube data available from the child cubes as well.
   * Both techniques allow to make the queries more efficient.
   *
   * The revision identifier specifies the index revision to optimize, if not
   * provided then the latest revision is used. In case of the staging area only
   * compaction is applied.
   *
   * The file limit defines how many files a cube should have to be included
   * into the compaction process.
   *
   * The overflow is the ratio (cubeSize - preferredCubeSize) /
   * preferredCubeSize, e.g. overflow 0.2 means that the cube has 1.2 times more
   * elements than the preferred cube size. Overflow limit defines the cubes
   * with too many elements and which need optimization.
   *
   * Those cubes which need replication should be specified explicitly. For such
   * cubes compaction is performed even if they meet neither file nor overflow
   * limit criteria.
   *
   * @param revisionID the revision identifier, if None then the lastest
   * revision is used
   * @param fileLimit: the file limit
   * @param overflowLimit the overflow limit
   * @param cubesToReplicate the identifiers of the cubes to replicate
   */
  def optimize(
      revisionID: Option[RevisionID] = None,
      fileLimit: Option[Int] = None,
      overflowLimit: Option[Double] = None,
      cubesToReplicate: Seq[CubeId] = Seq.empty): Unit = {
    throw new UnsupportedOperationException("Not implemented yet")
  }

  /**
   * The optimize operation should read the data of those cubes announced
   * and replicate it in their children
   * @param revisionID the identifier of the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   */
  def optimize(revisionID: RevisionID): Unit = {
    if (!isStaging(revisionID)) {
      checkRevisionAvailable(revisionID)
      OptimizeTableCommand(revisionID, indexedTable)
        .run(sparkSession)
    }
  }

  def optimize(): Unit = {
    if (!isStaging(latestRevisionAvailableID)) {
      optimize(latestRevisionAvailableID)
    }
  }

  /**
   * The analyze operation should analyze the index structure
   * and find the cubes that need optimization
   * @param revisionID the identifier of the revision to optimize.
   *                        If doesn't exist or none is specified, would be the last available
   * @return the sequence of cubes to optimize in string representation
   */
  def analyze(revisionID: RevisionID): Seq[String] = {
    if (isStaging(revisionID)) Seq.empty
    else {
      checkRevisionAvailable(revisionID)
      AnalyzeTableCommand(revisionID, indexedTable)
        .run(sparkSession)
        .map(_.getString(0))
    }
  }

  def analyze(): Seq[String] = {
    if (isStaging(latestRevisionAvailableID)) Seq.empty
    else analyze(latestRevisionAvailableID)
  }

  /**
   * The compact operation should compact the small files in the table
   * @param revisionID the identifier of the revision to optimize.
   *                        If doesn't exist or none is specified, would be the last available
   */
  def compact(revisionID: RevisionID): Unit = {
    checkRevisionAvailable(revisionID)
    CompactTableCommand(revisionID, indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

  def compact(): Unit = {
    compact(latestRevisionAvailableID)
  }

  /**
   * Outputs the indexed columns of the table
   * @param revisionID the identifier of the revision.
   *                          If doesn't exist or none is specified, would be the last available
   * @return
   */

  def indexedColumns(revisionID: RevisionID): Seq[String] = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot
      .loadRevision(revisionID)
      .columnTransformers
      .map(_.columnName)
  }

  def indexedColumns(): Seq[String] = {
    latestRevisionAvailable.columnTransformers.map(_.columnName)
  }

  /**
   * Outputs the cubeSize of the table
   * @param revisionID the identifier of the revision.
   *                          If doesn't exist or none is specified, would be the last available
   * @return
   */
  def cubeSize(revisionID: RevisionID): Int = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID).desiredCubeSize

  }

  def cubeSize(): Int =
    latestRevisionAvailable.desiredCubeSize

  /**
   * Outputs all the revision identifiers available for the table
   * @return
   */
  def revisionsIDs(): Seq[RevisionID] = {
    qbeastSnapshot.loadAllRevisions.map(_.revisionID)
  }

  /**
   * Outputs the identifier of the latest revision available
   * @return
   */
  def latestRevisionID(): RevisionID = {
    latestRevisionAvailableID
  }

  /**
   * Gather an overview of the index for a given revision
   * @param revisionID optional RevisionID
   * @return
   */
  def getIndexMetrics(revisionID: Option[RevisionID] = None): IndexMetrics = {
    val indexStatus = revisionID match {
      case Some(id) => qbeastSnapshot.loadIndexStatus(id)
      case None => qbeastSnapshot.loadLatestIndexStatus
    }

    val revision = indexStatus.revision
    val cubeStatuses = indexStatus.cubesStatuses

    val cubeCount = cubeStatuses.size
    val depth = if (cubeCount == 0) -1 else cubeStatuses.map(_._1.depth).max + 1
    val elementCount = cubeStatuses.flatMap(_._2.files.map(_.elementCount)).sum

    val indexingColumns = revision.columnTransformers.map(_.columnName)
    val dimensionCount = indexingColumns.size
    val desiredCubeSize = revision.desiredCubeSize

    val innerCs = cubeStatuses.filterKeys(_.children.exists(cubeStatuses.contains))

    val avgFanout = if (innerCs.nonEmpty) {
      innerCs.keys.toSeq
        .map(_.children.count(cubeStatuses.contains))
        .sum / innerCs.size.toDouble
    } else 0d

    IndexMetrics(
      cubeStatuses,
      dimensionCount,
      elementCount,
      depth,
      cubeCount,
      desiredCubeSize,
      indexingColumns.mkString(","),
      avgFanout,
      depthOnBalance(depth, cubeCount, dimensionCount),
      CubeSizeMetrics(innerCs, desiredCubeSize))
  }

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(sparkSession, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}
