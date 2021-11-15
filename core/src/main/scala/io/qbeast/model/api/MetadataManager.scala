package io.qbeast.model.api

import io.qbeast.IISeq
import io.qbeast.model.{IndexStatusChange, QTableID, RevisionChange, TableChanges}

trait MetadataManager[DataSchema, FileAction] {

  /**
   * Get the QbeastSnapshot for a given table
   * @param qtable
   * @return
   */
  def loadQbeastSnapshot(qtable: QTableID): QbeastSnapshot

  /**
   * Get the Schema for a given table
   * @param qtable
   */
  def loadCurrentSchema(qtable: QTableID): DataSchema

  /**
   * Save methods
   */

  /**
   * Perform an Update operation by using transaction control
   * @param writer the writer code to be executed
   * @param schema the schema of the data
   * @param qtable the QTableID
   * @param append the append flag
   */
  def updateWithTransaction(qtable: QTableID, schema: DataSchema, append: Boolean)(
      writer: => (TableChanges, IISeq[FileAction]))

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
