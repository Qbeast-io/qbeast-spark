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
   * @param tableID
   *   the QTableID
   * @return
   *   the current snapshot
   */
  def loadSnapshot(tableID: QTableID): QbeastSnapshot

  /**
   * Gets the Schema for a given table
   * @param tableID
   *   the QTableID
   * @return
   *   the current schema
   */
  def loadCurrentSchema(tableID: QTableID): DataSchema

  /**
   * Writes and updates the metadata by using transaction control
   * @param tableID
   *   the QTableID
   * @param schema
   *   the schema of the data
   * @param options
   *   the update options
   * @param append
   *   the append flag
   */
  def updateWithTransaction(
      tableID: QTableID,
      schema: DataSchema,
      options: QbeastOptions,
      append: Boolean)(writer: => (TableChanges, IISeq[FileDescriptor])): Unit

  /**
   * Updates the table metadata by overwriting the metadata configurations with the provided
   * key-value pairs.
   * @param tableID
   *   QTableID
   * @param schema
   *   table schema
   * @param update
   *   configurations used to overwrite the existing metadata
   */
  def updateMetadataWithTransaction(tableID: QTableID, schema: DataSchema)(
      update: => Configuration): Unit

  /**
   * Updates the Revision with the given RevisionChanges
   * @param tableID
   *   the QTableID
   * @param revisionChange
   *   the collection of RevisionChanges
   */
  def updateRevision(tableID: QTableID, revisionChange: RevisionChange): Unit

  /**
   * Updates the Table with the given TableChanges
   * @param tableID
   *   the QTableID
   * @param tableChanges
   *   the collection of TableChanges
   */
  def updateTable(tableID: QTableID, tableChanges: TableChanges): Unit

  /**
   * This function checks if there's a conflict. A conflict happens if there are new cubes that
   * have been optimized but they were not announced.
   *
   * @param tableID
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
      tableID: QTableID,
      revisionID: RevisionID,
      knownAnnounced: Set[CubeId],
      oldReplicatedSet: ReplicatedSet): Boolean

  /**
   * Checks if there's an existing log directory for the table
   * @param tableID
   *   the table ID
   * @return
   */
  def existsLog(tableID: QTableID): Boolean

  /**
   * Creates an initial log directory
   * @param tableID
   */
  def createLog(tableID: QTableID): Unit

}
