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

/**
 * Spark+Delta implementation of the MetadataManager interface
 */
object SparkDeltaMetadataManager extends MetadataManager[StructType, FileAction] {

  override def updateWithTransaction(tableID: QTableID, schema: StructType, append: Boolean)(
      writer: => (TableChanges, IISeq[FileAction])): Unit = {

    val deltaLog = DeltaLog.forTable(SparkSession.active, tableID.id)
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options =
      new DeltaOptions(Map("path" -> tableID.id), SparkSession.active.sessionState.conf)
    val metadataWriter = DeltaMetadataWriter(tableID, mode, deltaLog, options, schema)
    metadataWriter.writeWithTransaction(writer)
  }

  override def loadQbeastSnapshot(tableID: QTableID): DeltaQbeastSnapshot = {
    DeltaQbeastSnapshot(DeltaLog.forTable(SparkSession.active, tableID.id).update())
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    DeltaLog.forTable(SparkSession.active, tableID.id).update().schema
  }

  override def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit = {}

  override def updateIndexStatus(
      tableID: QTableID,
      indexStatusChange: IndexStatusChange): Unit = {}

  override def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit = {}
}
