/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

class SparkDeltaMetadataManager extends MetadataManager[QTableID] {

  override def loadAllRevisions(qtable: QTableID): IISeq[Revision] = {
    QbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
  }.revisions

  override def loadRevisionAt(qtable: QTableID, timestamp: Long): Revision = null

  override def updateRevision(revisionChange: RevisionChange): Unit = {}

  override def updateIndexStatus(indexStatusChange: IndexStatusChange): Unit = {}

  override def updateTable(tableChanges: TableChanges): Unit = {}

  override def updateWithTransaction(code: Function[_, TableChanges]): Unit = {}

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

}
