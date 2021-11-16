package io.qbeast.model.api

import io.qbeast.IISeq
import io.qbeast.model.{IndexStatusChange, QTableID, RevisionChange, TableChanges}

trait MetadataManager[DataSchema, FileAction] {

  /**
   * Get the QbeastSnapshot for a given table
   * @param tableID
   * @return
   */
  def loadQbeastSnapshot(tableID: QTableID): QbeastSnapshot

  /**
   * Get the Schema for a given table
   * @param tableID
   */
  def loadCurrentSchema(tableID: QTableID): DataSchema

  /**
   * Save methods
   */

  /**
   * Perform an Update operation by using transaction control
   * @param writer the writer code to be executed
   * @param schema the schema of the data
   * @param tableID the QTableID
   * @param append the append flag
   */
  def updateWithTransaction(tableID: QTableID, schema: DataSchema, append: Boolean)(
      writer: => (TableChanges, IISeq[FileAction]))

  /**
   * Update the Revision with the given RevisionChanges
   * @param revisionChange the collection of RevisionChanges
   * @param tableID the QTableID
   */
  def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   * @param indexStatusChange the collection of IndexStatusChanges
   * @param tableID the QTableID
   */
  def updateIndexStatus(tableID: QTableID, indexStatusChange: IndexStatusChange): Unit

  /**
   * Update the Table with the given TableChanges
   * @param tableChanges the collection of TableChanges
   * @param tableID the QTableID
   */
  def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit

}
