/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.{QTableID, RevisionID}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.table._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import io.qbeast.spark.internal.commands.AnalyzeTableCommand
import io.qbeast.spark.internal.commands.OptimizeTableCommand

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
    extends Serializable {

  private def deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableID.id)

  private def qbeastSnapshot: DeltaQbeastSnapshot =
    delta.DeltaQbeastSnapshot(deltaLog.snapshot)

  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableID)

  private def latestRevisionAvailable = qbeastSnapshot.loadLatestRevision.revisionID

  private def getAvailableRevision(revisionID: RevisionID): RevisionID = {
    if (qbeastSnapshot.existsRevision(revisionID)) revisionID
    else latestRevisionAvailable
  }

  /**
   * The optimize operation should read the data of those cubes announced
   * and replicate it in their children
   * @param revisionID the identifier of the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   */
  def optimize(revisionID: RevisionID = -1L): Unit = {
    OptimizeTableCommand(getAvailableRevision(revisionID), indexedTable)
      .run(sparkSession)

  }

  /**
   * The analyze operation should analyze the index structure
   * and find the cubes that need optimization
   * @param revisionID the identifier of the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   * @return the sequence of cubes to optimize in string representation
   */
  def analyze(revisionID: RevisionID = -1L): Seq[String] = {
    AnalyzeTableCommand(getAvailableRevision(revisionID), indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

  /**
   * Outputs the indexed columns of the table
   * @param revisionID the identifier of the revision.
   *                          If doesn't exist or none is specified, would be the last available
   * @return
   */
  def indexedColumns(revisionID: RevisionID = -1L): Seq[String] = {
    val revID = getAvailableRevision(revisionID)
    qbeastSnapshot.loadRevision(revID).columnTransformers.map(_.columnName)
  }

  /**
   * Outputs the cubeSize of the table
   * @param revisionID the identifier of the revision.
   *                          If doesn't exist or none is specified, would be the last available
   * @return
   */
  def cubeSize(revisionID: RevisionID = -1L): Int = {
    val revID = getAvailableRevision(revisionID)
    qbeastSnapshot.loadRevision(revID).desiredCubeSize
  }

  /**
   * Outputs all the revision identifiers available for the table
   * @return
   */
  def revisionsID(): Seq[RevisionID] = {
    qbeastSnapshot.loadAllRevisions.map(_.revisionID)
  }

  /**
   * Outputs the identifier of the latest revision available
   * @return
   */
  def latestRevisionID(): RevisionID = {
    latestRevisionAvailable
  }

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(sparkSession, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}
