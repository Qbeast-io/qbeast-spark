/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model._
import io.qbeast.model.api.MetadataManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.types.StructType

class SparkDeltaMetadataManager extends MetadataManager[StructType, FileAction] {

  /**
   * Update the metadata within a transaction
   * @param tableID the QTableID
   * @param schema the schema of the data
   * @param writer the writer code to be executed
   * @param append the append flag
   */
  override def updateWithTransaction(tableID: QTableID, schema: StructType, append: Boolean)(
      writer: => (TableChanges, IISeq[FileAction])): Unit = {

    val deltaLog = DeltaLog.forTable(SparkSession.active, tableID.id)
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options =
      new DeltaOptions(Map("path" -> tableID.id), SparkSession.active.sessionState.conf)
    val metadataWriter = DeltaMetadataWriter(tableID, mode, deltaLog, options, schema)
    metadataWriter.writeWithTransaction(writer)
  }

  /**
   * Get the QbeastSnapshot for a given table
   *
   * @param tableID
   * @return
   */
  override def loadQbeastSnapshot(tableID: QTableID): DeltaQbeastSnapshot = {
    DeltaQbeastSnapshot(DeltaLog.forTable(SparkSession.active, tableID.id).update())
  }

  /**
   * Get the Schema for a given table
   *
   * @param tableID
   */
  override def loadCurrentSchema(tableID: QTableID): StructType = {
    DeltaLog.forTable(SparkSession.active, tableID.id).update().schema
  }

  /**
   * Update the Revision with the given RevisionChanges
   *
   * @param revisionChange the collection of RevisionChanges
   * @param tableID         the QTableID
   */
  override def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit = {}

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   *
   * @param indexStatusChange the collection of IndexStatusChanges
   * @param tableID            the QTableID
   */
  override def updateIndexStatus(
      tableID: QTableID,
      indexStatusChange: IndexStatusChange): Unit = {}

  /**
   * Update the Table with the given TableChanges
   *
   * @param tableChanges the collection of TableChanges
   * @param tableID       the QTableID
   */
  override def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit = {}
}
