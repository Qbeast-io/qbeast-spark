package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Data Writer template
 * @tparam DATA
 *   type of data
 * @tparam DataSchema
 *   type of the data schema
 * @tparam FileDescriptor
 *   type of file descriptor
 */
trait DataWriter[DATA, DataSchema, FileDescriptor] {

  /**
   * Write the index data to the files
   * @param tableID
   *   the table identifier
   * @param schema
   *   the schema of the data
   * @param data
   *   the data to write
   * @param tableChanges
   *   the changes of the index
   * @return
   *   the sequence of files written
   */
  def write(
      tableID: QTableID,
      schema: DataSchema,
      data: DATA,
      tableChanges: TableChanges): IISeq[FileDescriptor]

  /**
   * Compact the files
   * @param tableID
   *   the table identifier
   * @param schema
   *   the schema of the data
   * @param revision
   *   the revision of the index
   * @param indexStatus
   *   the current index status
   * @param indexFiles
   *   the index files to compact
   * @return
   *   the sequence of files written and deleted
   */
  def compact(
      tableID: QTableID,
      schema: DataSchema,
      revision: Revision,
      indexStatus: IndexStatus,
      indexFiles: Seq[IndexFile]): IISeq[FileDescriptor]

}
