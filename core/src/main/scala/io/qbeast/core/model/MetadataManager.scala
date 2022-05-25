package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Metadata Manager template
 * @tparam DataSchema type of data schema
 * @tparam FileDescriptor type of file descriptor
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
   * Updates the Table with the given TableChanges
   * @param tableID the QTableID
   * @param tableChanges the collection of TableChanges
   */
  def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit

  /**
   * This function checks if there's a conflict. A conflict happens if there
   * are new cubes that have been optimized but they were not announced.
   *
   * @param tableID the table ID
   * @param revisionID the revision ID
   * @param knownAnnounced the cubes we know they were announced when the write operation started.
   * @param oldReplicatedSet the old replicated set
   * @return true if there is a conflict, false otherwise
   */
  def hasConflicts(
      tableID: QTableID,
      revisionID: RevisionID,
      knownAnnounced: Set[CubeId],
      oldReplicatedSet: ReplicatedSet): Boolean

}
