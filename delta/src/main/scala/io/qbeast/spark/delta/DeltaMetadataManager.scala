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
import io.qbeast.core.model.WriteMode.WriteModeValue
import io.qbeast.IISeq
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

/**
 * Spark+Delta implementation of the MetadataManager interface
 */
object DeltaMetadataManager extends MetadataManager {

  override def updateWithTransaction(
      tableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      writeMode: WriteModeValue)(
      writer: String => (TableChanges, IISeq[IndexFile], IISeq[DeleteFile])): Unit = {

    val deltaLog = loadDeltaLog(tableID)
    val metadataWriter =
      DeltaMetadataWriter(tableID, writeMode, deltaLog, options, schema)

    metadataWriter.writeWithTransaction(writer)
  }

  override def updateMetadataWithTransaction(
      tableID: QTableID,
      schema: StructType,
      overwrite: Boolean)(update: => Configuration): Unit = {
    val deltaLog = loadDeltaLog(tableID)
    val metadataWriter =
      DeltaMetadataWriter(tableID, WriteMode.Append, deltaLog, QbeastOptions.empty, schema)

    metadataWriter.updateMetadataWithTransaction(update, overwrite)

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

  /**
   * Checks if there's an existing log directory for the table
   *
   * @param tableID
   *   the table ID
   * @return
   */
  override def existsLog(tableID: QTableID): Boolean = {
    val path = new Path(tableID.id, "_delta_log")
    val fs = path.getFileSystem(SparkSession.active.sessionState.newHadoopConf())
    fs.exists(path)
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
