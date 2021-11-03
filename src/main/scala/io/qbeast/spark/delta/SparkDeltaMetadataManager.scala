/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model.{
  IndexStatus,
  IndexStatusChange,
  MetadataManager,
  QTableID,
  Revision,
  RevisionChange,
  RevisionID,
  TableChanges
}

class SparkDeltaMetadataManager extends MetadataManager {
  override def loadLatestIndexStatus(qtable: QTableID): IndexStatus = null

  override def loadAllIndexStatus(qtable: QTableID): IISeq[IndexStatus] = null

  override def loadAllRevisions(qtable: QTableID): IISeq[Revision] = null

  override def loadRevisionStatus(revisionID: RevisionID): IndexStatus = null

  override def loadRevisionAt(timestamp: Long): Revision = null

  override def loadRevisionStatusAt(timestamp: Long): IndexStatus = null

  override def updateRevision(revisionChange: RevisionChange): Unit = {}

  override def updateIndexStatus(indexStatusChange: IndexStatusChange): Unit = {}

  override def updateTable(tableChanges: TableChanges): Unit = {}

  override def updateWithTransaction(code: Function[_, TableChanges]): Unit = {}
}
