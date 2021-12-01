package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Metadata Manager template
 * @tparam DataSchema
 * @tparam FileDescriptor
 */
trait MetadataManager[DataSchema, FileDescriptor] {

  /**
   * Gets the Snapshot for a given table
   * @param tableID the QTableID
   * @return the current snapshot
   */
  def loadSnapshot(tableID: QTableID): QbeastSnapshot

  /**
   * Gets the Schema for a given table
   * @param tableID the QTableID
   * @return the current schema
   */
  def loadCurrentSchema(tableID: QTableID): DataSchema

  /**
   * Writes and updates the metadata by using transaction control
   * @param tableID the QTableID
   * @param schema the schema of the data
   * @param append the append flag
   * @param writer the writer code to be executed
   */
  def updateWithTransaction(tableID: QTableID, schema: DataSchema, append: Boolean)(
      writer: => (TableChanges, IISeq[FileDescriptor])): Unit

  /**
   * Updates the Revision with the given RevisionChanges
   * @param tableID the QTableID
   * @param revisionChange the collection of RevisionChanges
   */
  def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit

  /**
   * Updates the IndexStatus with the given IndexStatusChanges
   * @param tableID the QTableID
   * @param indexStatusChange the collection of IndexStatusChanges
   */
  def updateIndexStatus(tableID: QTableID, indexStatusChange: IndexStatusChange): Unit

  /**
   * Updates the Table with the given TableChanges
   * @param tableID the QTableID
   * @param tableChanges the collection of TableChanges
   */
  def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit

}
