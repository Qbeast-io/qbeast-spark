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
package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Metadata Manager template
 * @tparam DataSchema
 *   type of data schema
 * @tparam FileDescriptor
 *   type of file descriptor
 * @tparam QbeastOptions
 *   type of the Qbeast options
 */
trait MetadataManager[DataSchema, FileDescriptor, QbeastOptions] {
  type Configuration = Map[String, String]

  /**
   * Gets the Snapshot for a given table
   * @param tableId
   *   the QTableID
   * @return
   *   the current snapshot
   */
  def loadSnapshot(tableId: QTableId): QbeastSnapshot

  /**
   * Gets the Schema for a given table
   * @param tableId
   *   the QTableID
   * @return
   *   the current schema
   */
  def loadCurrentSchema(tableId: QTableId): DataSchema

  /**
   * Writes and updates the metadata by using transaction control
   * @param tableId
   *   the QTableID
   * @param schema
   *   the schema of the data
   * @param options
   *   the update options
   * @param append
   *   the append flag
   */
  def updateWithTransaction(
      tableId: QTableId,
      schema: DataSchema,
      options: QbeastOptions,
      append: Boolean)(writer: => (TableChanges, IISeq[FileDescriptor])): Unit

  /**
   * Updates the table metadata by overwriting the metadata configurations with the provided
   * key-value pairs.
   * @param tableId
   *   QTableID
   * @param schema
   *   table schema
   * @param update
   *   configurations used to overwrite the existing metadata
   */
  def updateMetadataWithTransaction(tableId: QTableId, schema: DataSchema)(
      update: => Configuration): Unit

  /**
   * Updates the Revision with the given RevisionChanges
   * @param tableId
   *   the QTableID
   * @param revisionChange
   *   the collection of RevisionChanges
   */
  def updateRevision(tableId: QTableId, revisionChange: RevisionChange): Unit

  /**
   * Updates the Table with the given TableChanges
   * @param tableId
   *   the QTableID
   * @param tableChanges
   *   the collection of TableChanges
   */
  def updateTable(tableId: QTableId, tableChanges: TableChanges): Unit

  /**
   * This function checks if there's a conflict. A conflict happens if there are new cubes that
   * have been optimized, but they were not announced.
   *
   * @param tableId
   *   the table ID
   * @param revisionID
   *   the revision ID
   * @param knownAnnounced
   *   the cubes we know they were announced when the write operation started.
   * @param oldReplicatedSet
   *   the old replicated set
   * @return
   *   true if there is a conflict, false otherwise
   */
  def hasConflicts(
      tableId: QTableId,
      revisionID: RevisionID,
      knownAnnounced: Set[CubeId],
      oldReplicatedSet: ReplicatedSet): Boolean

  /**
   * Checks if there's an existing log directory for the table
   * @param tableId
   *   the table ID
   * @return
   */
  def existsLog(tableId: QTableId): Boolean

  /**
   * Creates an initial log directory
   * @param tableId
   */
  def createLog(tableId: QTableId): Unit

}
