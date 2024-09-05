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

import io.qbeast.core.metadata.MetadataManager
import io.qbeast.core.metadata.MetadataManagerFactory
import io.qbeast.core.metadata.SparkMetadataManager
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
class DeltaSparkMetadataManager extends SparkMetadataManager[FileAction] {

  override def updateWithTransaction(
      tableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      append: Boolean)(writer: => (TableChanges, IISeq[FileAction])): Unit = {

    val deltaLog = loadDeltaQbeastLog(tableID).deltaLog
    val mode = if (append) SaveMode.Append else SaveMode.Overwrite

    val metadataWriter =
      DeltaMetadataWriter(tableID, mode, deltaLog, options, schema)
    metadataWriter.writeWithTransaction(writer)
  }

  override def updateMetadataWithTransaction(tableID: QTableID, schema: StructType)(
      update: => Configuration): Unit = {
    val deltaLog = loadDeltaQbeastLog(tableID).deltaLog
    val metadataWriter =
      DeltaMetadataWriter(tableID, mode = SaveMode.Append, deltaLog, QbeastOptions.empty, schema)

    metadataWriter.updateMetadataWithTransaction(update)

  }

  override def loadSnapshot(tableID: QTableID): DeltaQbeastSnapshot = {
    new DeltaQbeastSnapshot(tableID)
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    loadDeltaQbeastLog(tableID).deltaLog.update().schema
  }

  override def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit = {}

  override def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit = {}

  /**
   * Returns the DeltaQbeastLog for the table
   * @param tableID
   *   the table ID
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

  /**
   * Checks if there's an existing log directory for the table
   *
   * @param tableID
   *   the table ID
   * @return
   */
  override def existsLog(tableID: QTableID): Boolean = {
    loadDeltaQbeastLog(tableID).deltaLog.tableExists
  }

  /**
   * Creates an initial log directory
   *
   * @param tableID
   */
  override def createLog(tableID: QTableID): Unit = {
    loadDeltaQbeastLog(tableID).deltaLog.createLogDirectory()
  }

}

class DeltaSparkMetadataManagerFactory
    extends MetadataManagerFactory[StructType, FileAction, QbeastOptions] {

  override def createMetadataManager(): MetadataManager[StructType, FileAction, QbeastOptions] = {
    new DeltaSparkMetadataManager()
  }

  override val format: String = "delta"
}
