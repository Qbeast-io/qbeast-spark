package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Metadata Manager template
 * @tparam DataSchema type of data schema
 * @tparam FileDescriptor type of file descriptor
 */
trait MetadataManager[DataSchema, FileDescriptor] {
  type Configuration = Map[String, String]

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
   * Updates the table metadata by overwriting the metadata configurations
   * with the provided key-value pairs.
   * @param tableID QTableID
   * @param schema table schema
   * @param update configurations used to overwrite the existing metadata
   */
  def updateMetadataWithTransaction(tableID: QTableID, schema: DataSchema)(
      update: => Configuration): Unit

  /**
   * Updates the Revision with the given RevisionChanges
   * @param tableID the QTableID
   * @param revisionChange the collection of RevisionChanges
   */
  def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit

  /**
   * Updates the Table with the given TableChanges
   * @param tableID the QTableID
   * @param tableChanges the collection of TableChanges
   */
  def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit

  /**
   * Checks if there's an existing log directory for the table
   * @param tableID the table ID
   * @return
   */
  def existsLog(tableID: QTableID): Boolean

  /**
   * Creates an initial log directory
   * @param tableID
   */
  def createLog(tableID: QTableID): Unit

}
