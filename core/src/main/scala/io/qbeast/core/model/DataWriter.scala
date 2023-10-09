package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Data Writer template
 * @tparam DATA type of data
 * @tparam DataSchema type of the data schema
 * @tparam FileDescriptor type of file descriptor
 */
trait DataWriter[DATA, DataSchema, FileDescriptor] {

  /**
   * Write the index data to the files
   * @param tableID the table identifier
   * @param schema the schema of the data
   * @param data the data to write
   * @param tableChanges the changes of the index
   * @return the sequence of files written
   */
  def write(
      tableID: QTableID,
      schema: DataSchema,
      data: DATA,
      tableChanges: TableChanges): IISeq[FileDescriptor]

  /**
   * Compacts the files of the specified index revision.
   *
   * @param tableId the table identifier
   * @param schema the table schema
   * @param revisionId the index revision
   * @param indexFiles the index files to compact
   * @param desiredFileSize the desired minimum number of elements in a single index file
   * after compaction
   * @return the sequence of files written and deleted
   */
  def compact(
      tableId: QTableID,
      schema: DataSchema,
      revisionId: RevisionID,
      indexFiles: IISeq[IndexFile],
      desiredFileSize: Long): IISeq[FileDescriptor]

}
