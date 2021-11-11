package io.qbeast.model

import io.qbeast.IISeq

trait MetadataManager[DataSchema, FileAction] {

  /**
   * Load methods
   */

  /**
   * Obtains the latest IndexStatus for a given QTableID
   * @param qtable the QTableID
   * @return the latest IndexStatus for qtable
   */
  def loadIndexStatus(qtable: QTableID): IndexStatus

  /**
   * Obtains the latest IndexStatus for a given QTableID and RevisionID
   * @param qtable the QTableID
   * @param revisionID the RevisionID
   * @return
   */
  def loadIndexStatusAt(qtable: QTableID, revisionID: RevisionID): IndexStatus

  /**
   * Obtain all Revisions for a given QTableID
   * @param qtable the QTableID
   * @return an immutable Seq of Revision for qtable
   */
  def loadAllRevisions(qtable: QTableID): IISeq[Revision]

  /**
   * Obtain the last Revisions for a given QTableID
   * @param qtable the QTableID
   * @return an immutable Seq of Revision for qtable
   */
  def loadLatestRevision(qtable: QTableID): Revision

  /**
   * Obtain the IndexStatus for a given RevisionID
   * @param revisionID the RevisionID
   * @return the IndexStatus for revisionID
   */
  def loadRevision(qtable: QTableID, revisionID: RevisionID): Revision

  /**
   * Loads the most updated revision at a given timestamp
   * @param timestamp the timestamp in Long format
   * @return the latest Revision at a concrete timestamp
   */
  def loadRevisionAt(qtable: QTableID, timestamp: Long): Revision

  /**
   * Save methods
   */

  /**
   * Perform an Update operation by using transaction control
   * @param code the code to be executed
   * @param schema the schema of the data
   * @param qtable the QTableID
   * @param append the append flag
   */
  def updateWithTransaction(
      qtable: QTableID,
      schema: DataSchema,
      code: => (TableChanges, IISeq[FileAction]),
      append: Boolean): Unit

  /**
   * Update the Revision with the given RevisionChanges
   * @param revisionChange the collection of RevisionChanges
   * @param qtable the QTableID
   */
  def updateRevision(qtable: QTableID, revisionChange: RevisionChange): Unit

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   * @param indexStatusChange the collection of IndexStatusChanges
   * @param qtable the QTableID
   */
  def updateIndexStatus(qtable: QTableID, indexStatusChange: IndexStatusChange): Unit

  /**
   * Update the Table with the given TableChanges
   * @param tableChanges the collection of TableChanges
   * @param qtable the QTableID
   */
  def updateTable(qtable: QTableID, tableChanges: TableChanges): Unit

}
