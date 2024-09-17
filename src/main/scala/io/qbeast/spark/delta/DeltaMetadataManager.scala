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
object DeltaMetadataManager extends MetadataManager[StructType, IndexFile, QbeastOptions] {

  override def updateWithTransaction(
      tableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      append: Boolean)(writer: => (TableChanges, IISeq[IndexFile])): Unit = {

    val deltaLog = loadDeltaLog(tableID)
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite

    val metadataWriter =
      DeltaMetadataWriter(tableID, mode, deltaLog, options, schema)

    val deltaWriter: (TableChanges, Seq[FileAction]) = {
      val (tableChanges, indexFiles) = writer
      val fileActions = indexFiles.map(IndexFiles.toFileAction)
      (tableChanges, fileActions)
    }

    metadataWriter.writeWithTransaction(deltaWriter)
  }

  override def updateMetadataWithTransaction(tableID: QTableID, schema: StructType)(
      update: => Configuration): Unit = {
    val deltaLog = loadDeltaLog(tableID)
    val metadataWriter =
      DeltaMetadataWriter(tableID, mode = SaveMode.Append, deltaLog, QbeastOptions.empty, schema)

    metadataWriter.updateMetadataWithTransaction(update)

  }

  override def loadSnapshot(tableID: QTableID): DeltaQbeastSnapshot = {
    DeltaQbeastSnapshot(tableID)
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    loadDeltaLog(tableID).update().schema
  }

  override def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit = {}

  override def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit = {}

  /**
   * Returns the DeltaQbeastLog for the table
   * @param tableID
   *   the table ID
   * @return
   */
  def loadDeltaLog(tableID: QTableID): DeltaLog = {
    DeltaLog.forTable(SparkSession.active, tableID.id)
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

  /**
   * Checks if there's an existing log directory for the table
   *
   * @param tableID
   *   the table ID
   * @return
   */
  override def existsLog(tableID: QTableID): Boolean = {
    loadDeltaLog(tableID).tableExists
  }

  /**
   * Creates an initial log directory
   *
   * @param tableID
   *   Table ID
   */
  override def createLog(tableID: QTableID): Unit = {
    loadDeltaLog(tableID).createLogDirectory()
  }

}
