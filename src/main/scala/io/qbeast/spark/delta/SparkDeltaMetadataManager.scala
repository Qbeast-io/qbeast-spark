/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.core.model.{MetadataManager, _}
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

    val deltaLog = loadDeltaQbeastLog(tableID).deltaLog
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite
    val options =
      new DeltaOptions(Map("path" -> tableID.id), SparkSession.active.sessionState.conf)
    val metadataWriter = DeltaMetadataWriter(tableID, mode, deltaLog, options, schema)
    metadataWriter.writeWithTransaction(writer)
  }

  override def loadSnapshot(tableID: QTableID): DeltaQbeastSnapshot = {
    DeltaQbeastSnapshot(loadDeltaQbeastLog(tableID).deltaLog.update())
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    loadDeltaQbeastLog(tableID).deltaLog.update().schema
  }

  override def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit = {}

  override def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit = {}

  /**
   * Returns the DeltaQbeastLog for the table
   * @param tableID the table ID
   * @return
   */
  def loadDeltaQbeastLog(tableID: QTableID): DeltaQbeastLog = {
    DeltaQbeastLog(DeltaLog.forTable(SparkSession.active, tableID.id))
  }

  override def hasConflicts(
      tableID: QTableID,
      revisionID: RevisionID,
      knownAnnounced: Set[CubeId],
      oldReplicatedSet: ReplicatedSet): Boolean = {

    val snapshot = loadSnapshot(tableID)
    if (snapshot.isInitial) return false

    val newReplicatedSet = snapshot.loadIndexStatus(revisionID).replicatedSet
    val deltaReplicatedSet = newReplicatedSet -- oldReplicatedSet
    val diff = deltaReplicatedSet -- knownAnnounced
    diff.nonEmpty
  }

}
