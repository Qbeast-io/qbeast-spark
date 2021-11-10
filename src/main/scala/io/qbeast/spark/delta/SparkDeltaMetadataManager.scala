/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaOptions}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.types.StructType

class SparkDeltaMetadataManager extends MetadataManager[QTableID, StructType, FileAction] {

  /**
   * Returns the sequence of files for a set of cubes belonging to a specific space revision
   *
   * @param cubes the set of cubes
   * @return the sequence of blocks
   */
  def getCubeFiles(
      qtable: QTableID,
      indexStatus: IndexStatus,
      cubes: Set[CubeId]): Seq[AddFile] = {
    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
      .getCubeBlocks(indexStatus, cubes)
  }

  override def loadAllRevisions(qtable: QTableID): IISeq[Revision] = {
    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
  }.revisions

  override def loadRevisionAt(qtable: QTableID, timestamp: Long): Revision = null

  /**
   * Obtain the last Revisions for a given QTableID
   *
   * @param qtable the QTableID
   * @return an immutable Seq of Revision for qtable
   */
  override def loadLatestRevision(qtable: QTableID): Revision = {
    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
  }.lastRevision

  /**
   * Obtains the latest IndexStatus for a given QTableID
   *
   * @param qtable the QTableID
   * @return the latest IndexStatus for qtable, or None if no IndexStatus exists for qtable
   */
  override def loadIndexStatus(qtable: QTableID): IndexStatus = {

    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot).lastRevisionData
  }

  override def loadIndexStatusAt(qtable: QTableID, revisionID: RevisionID): IndexStatus = {
    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
      .getRevisionData(revisionID)
  }

  /**
   * Obtain the IndexStatus for a given RevisionID
   *
   * @param revisionID the RevisionID
   * @return the IndexStatus for revisionID
   */
  override def loadRevision(qtable: QTableID, revisionID: RevisionID): Revision = {
    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
      .getRevision(revisionID)
  }

  override def updateWithTransaction(
      qtable: QTableID,
      schema: StructType,
      code: => (TableChanges, IISeq[FileAction]),
      append: Boolean): Unit = {

    val spark = SparkSession.active
    val deltaLog = DeltaLog.forTable(SparkSession.active, qtable.id)
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options =
      new DeltaOptions(Map("path" -> qtable.id), spark.sessionState.conf)
    val deltaOperation = {
      DeltaOperations.Write(mode, None, options.replaceWhere, options.userMetadata)
    }
    val metadataWriter = DeltaMetadataWriter(qtable, mode, deltaLog, options, schema)

    deltaLog.withNewTransaction { txn =>
      val (changes, newFiles) = code
      val finalActions = metadataWriter.updateMetadata(txn, changes, newFiles)
      txn.commit(finalActions, deltaOperation)
    }
  }

  /**
   * Update the Revision with the given RevisionChanges
   *
   * @param revisionChange the collection of RevisionChanges
   * @param qtable         the QTableID
   */
  override def updateRevision(qtable: QTableID, revisionChange: RevisionChange): Unit = {}

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   *
   * @param indexStatusChange the collection of IndexStatusChanges
   * @param qtable            the QTableID
   */
  override def updateIndexStatus(
      qtable: QTableID,
      indexStatusChange: IndexStatusChange): Unit = {}

  /**
   * Update the Table with the given TableChanges
   *
   * @param tableChanges the collection of TableChanges
   * @param qtable       the QTableID
   */
  override def updateTable(qtable: QTableID, tableChanges: TableChanges): Unit = {}
}
