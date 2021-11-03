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
  override def loadLatestIndexStatus(qtable: QTableID): IndexStatus = _

  override def loadAllIndexStatus(qtable: QTableID): IISeq[IndexStatus] = _

  override def loadAllRevisions(qtable: QTableID): IISeq[Revision] = _

  override def loadRevisionStatus(revisionID: RevisionID): IndexStatus = _

  /**
   * Loads the most updated revision at a given timestamp
   *
   * @param timestamp
   * @return the latest Revision at a concrete timestamp
   */
  override def loadRevisionAt(timestamp: Long): Revision = { _ }

  override def loadRevisionStatusAt(timestamp: Long): IndexStatus = { _ }

  override def updateRevision(revisionChange: RevisionChange): Unit = {}

  override def updateIndexStatus(indexStatusChange: IndexStatusChange): Unit = {}

  override def updateTable(tableChanges: TableChanges): Unit = {}

  override def updateWithTransaction(code: Function[_, TableChanges]): Unit = {}
}
