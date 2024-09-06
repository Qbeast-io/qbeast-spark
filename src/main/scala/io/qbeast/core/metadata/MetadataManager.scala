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
package io.qbeast.core.metadata

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastSnapshot
import io.qbeast.core.model.ReplicatedSet
import io.qbeast.core.model.RevisionChange
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.TableChanges
import io.qbeast.IISeq

import java.util.ServiceLoader

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

object MetadataManager {

  /**
   * Creates a MetadataManager instance for a given configuration.
   *
   * @param format
   *   the storage format
   * @return
   *   a MetadataManager instance
   */
  def apply[DataSchema, FileDescriptor, QbeastOptions](
      format: String): MetadataManager[DataSchema, FileDescriptor, QbeastOptions] = {

    val loader = ServiceLoader.load(
      classOf[MetadataManagerFactory[DataSchema, FileDescriptor, QbeastOptions]])
    val iterator = loader.iterator()

    while (iterator.hasNext) {
      val factory = iterator.next()

      if (factory.format.equalsIgnoreCase(format)) {
        return factory.createMetadataManager()
      }
    }

    throw new IllegalArgumentException(s"No MetadataManagerFactory found for format: $format")

  }

}

/**
 * Factory for creating MetadataManager instances. This interface should be implemented and
 * deployed by external libraries as follows: <ul> <li>Implement this interface in a class which
 * has public no-argument constructor</li> <li>Register the implementation according to
 * ServiceLoader specification</li> <li>Add the jar with the implementation to the application
 * classpath</li> </ul>
 */
trait MetadataManagerFactory[DataSchema, FileDescriptor, QbeastOptions] {

  /**
   * Creates a new MetadataManager for a given configuration.
   *
   * @return
   *   a new MetadataManager
   */
  def createMetadataManager(): MetadataManager[DataSchema, FileDescriptor, QbeastOptions]

  val format: String
}
