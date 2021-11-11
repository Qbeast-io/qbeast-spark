/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.types.StructType

class SparkDeltaMetadataManager extends MetadataManager[StructType, FileAction] {

  /**
   * Update the metadata within a transaction
   * @param qtable the QTableID
   * @param schema the schema of the data
   * @param writer the writer code to be executed
   * @param append the append flag
   */
  override def updateWithTransaction(qtable: QTableID, schema: StructType, append: Boolean)(
      writer: => (TableChanges, IISeq[FileAction])): Unit = {

    val deltaLog = DeltaLog.forTable(SparkSession.active, qtable.id)
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options =
      new DeltaOptions(Map("path" -> qtable.id), SparkSession.active.sessionState.conf)
    val metadataWriter = DeltaMetadataWriter(qtable, mode, deltaLog, options, schema)
    metadataWriter.writeWithTransaction(writer)
  }

  /**
   * Update the Revision with the given RevisionChanges
   *
   * @param revisionChange the collection of RevisionChanges
   * @param qtable         the QTableID
   */
  override def updateRevision(qtable: QTableID, revisionChange: RevisionChange): Unit = {}

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   *
   * @param indexStatusChange the collection of IndexStatusChanges
   * @param qtable            the QTableID
   */
  override def updateIndexStatus(
      qtable: QTableID,
      indexStatusChange: IndexStatusChange): Unit = {}

  /**
   * Update the Table with the given TableChanges
   *
   * @param tableChanges the collection of TableChanges
   * @param qtable       the QTableID
   */
  override def updateTable(qtable: QTableID, tableChanges: TableChanges): Unit = {}

  /**
   * Get the QbeastSnapshot for a given table
   *
   * @param qtable
   * @return
   */
  override def loadQbeastSnapshot(qtable: QTableID): DeltaQbeastSnapshot = {
    DeltaQbeastSnapshot(DeltaLog.forTable(SparkSession.active, qtable.id).snapshot)
  }

}
