package io.qbeast.model

import io.qbeast.IISeq

trait MetadataManager[DataSchema, FileAction] {

  /**
   * Gets the Snapshot for a given table
   * @param tableID the table identifier
   * @return the current snapshot
   */
  def loadSnapshot(tableID: QTableID): QbeastSnapshot

  /**
   * Gets the Schema for a given table
   * @param tableID the table identifier
   * @return the current schema
   */
  def loadCurrentSchema(tableID: QTableID): DataSchema

  /**
   * Save methods
   */

  /**
   * Writes and updates the metadata by using transaction control
   * @param writer the writer code to be executed
   * @param schema the schema of the data
   * @param tableID the QTableID
   * @param append the append flag
   */
  def updateWithTransaction(tableID: QTableID, schema: DataSchema, append: Boolean)(
      writer: => (TableChanges, IISeq[FileAction])): Unit

  /**
   * Updates the Revision with the given RevisionChanges
   * @param revisionChange the collection of RevisionChanges
   * @param tableID the QTableID
   */
  def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit

  /**
   * Updates the IndexStatus with the given IndexStatusChanges
   * @param indexStatusChange the collection of IndexStatusChanges
   * @param tableID the QTableID
   */
  def updateIndexStatus(tableID: QTableID, indexStatusChange: IndexStatusChange): Unit

  /**
   * Updates the Table with the given TableChanges
   * @param tableChanges the collection of TableChanges
   * @param tableID the QTableID
   */
  def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit

}
