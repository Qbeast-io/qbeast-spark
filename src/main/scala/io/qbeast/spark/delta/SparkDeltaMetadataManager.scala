/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.IISeq
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * Spark+Delta implementation of the MetadataManager interface
 */
object SparkDeltaMetadataManager extends MetadataManager[StructType, FileAction, QbeastOptions] {

  override def updateWithTransaction(
      tableId: QTableId,
      schema: StructType,
      options: QbeastOptions,
      append: Boolean)(writer: => (TableChanges, IISeq[FileAction])): Unit = {

    val deltaLog = loadDeltaQbeastLog(tableId).deltaLog
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite

    val metadataWriter =
      DeltaMetadataWriter(tableId, mode, deltaLog, options, schema)
    metadataWriter.writeWithTransaction(writer)
  }

  override def updateMetadataWithTransaction(tableId: QTableId, schema: StructType)(
      update: => Configuration): Unit = {
    val deltaLog = loadDeltaQbeastLog(tableId).deltaLog
    val metadataWriter =
      DeltaMetadataWriter(tableId, mode = SaveMode.Append, deltaLog, QbeastOptions.empty, schema)

    metadataWriter.updateMetadataWithTransaction(update)

  }

  override def loadSnapshot(tableId: QTableId): DeltaQbeastSnapshot = {
    DeltaQbeastSnapshot(loadDeltaQbeastLog(tableId).deltaLog.update())
  }

  override def loadCurrentSchema(tableId: QTableId): StructType = {
    loadDeltaQbeastLog(tableId).deltaLog.update().schema
  }

  override def updateRevision(tableId: QTableId, revisionChange: RevisionChange): Unit = {}

  override def updateTable(tableId: QTableId, tableChanges: TableChanges): Unit = {}

  /**
   * Returns the DeltaQbeastLog for the table
   * @param tableId
   *   the table ID
   * @return
   */
  def loadDeltaQbeastLog(tableId: QTableId): DeltaQbeastLog = {
    DeltaQbeastLog(DeltaLog.forTable(SparkSession.active, tableId.id))
  }

  override def hasConflicts(
                             tableId: QTableId,
                             revisionId: RevisionId,
                             knownAnnounced: Set[CubeId],
                             oldReplicatedSet: ReplicatedSet): Boolean = {

    val snapshot = loadSnapshot(tableId)
    if (snapshot.isInitial) return false

    val newReplicatedSet = snapshot.loadIndexStatus(revisionId).replicatedSet
    val deltaReplicatedSet = newReplicatedSet -- oldReplicatedSet
    val diff = deltaReplicatedSet -- knownAnnounced
    diff.nonEmpty
  }

  /**
   * Checks if there's an existing log directory for the table
   *
   * @param tableId
   *   the table ID
   * @return
   */
  override def existsLog(tableId: QTableId): Boolean = {
    loadDeltaQbeastLog(tableId).deltaLog.tableExists
  }

  /**
   * Creates an initial log directory
   *
   * @param tableId
   */
  override def createLog(tableId: QTableId): Unit = {
    loadDeltaQbeastLog(tableId).deltaLog.createLogDirectory()
  }

}
