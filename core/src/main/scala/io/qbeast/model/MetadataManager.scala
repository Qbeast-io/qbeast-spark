package io.qbeast.model

import io.qbeast.IISeq

trait MetadataManager[T <: QTableID] {

  /**
   * Obtains the latest IndexStatus for a given QTableID
   * @param qtable the QTableID
   * @return the latest IndexStatus for qtable
   */
  def loadIndexStatus(qtable: T): IndexStatus

  /**
   * Obtain all Revisions for a given QTableID
   * @param qtable the QTableID
   * @return an immutable Seq of Revision for qtable
   */
  def loadAllRevisions(qtable: T): IISeq[Revision]

  /**
   * Obtain the last Revisions for a given QTableID
   * @param qtable the QTableID
   * @return an immutable Seq of Revision for qtable
   */
  def loadLatestRevision(qtable: T): Revision

  /**
   * Obtain the IndexStatus for a given RevisionID
   * @param revisionID the RevisionID
   * @return the IndexStatus for revisionID
   */
  def loadRevision(qtable: T, revisionID: RevisionID): Revision

  /**
   * Loads the most updated revision at a given timestamp
   * @param timestamp the timestamp in Long format
   * @return the latest Revision at a concrete timestamp
   */
  def loadRevisionAt(qtable: T, timestamp: Long): Revision

  /**
   * Update the Revision with the given RevisionChanges
   * @param revisionChange the collection of RevisionChanges
   */
  def updateRevision(revisionChange: RevisionChange): Unit

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   * @param indexStatusChange the collection of IndexStatusChanges
   */
  def updateIndexStatus(indexStatusChange: IndexStatusChange): Unit

  /**
   * Update the Table with the given TableChanges
   * @param tableChanges the collection of TableChanges
   */
  def updateTable(tableChanges: TableChanges): Unit

  /**
   * Perform an Update operation by using transcation control provided by Delta
   * @param code the code to be executed
   */
  def updateWithTransaction(code: _ => TableChanges): Unit

}
