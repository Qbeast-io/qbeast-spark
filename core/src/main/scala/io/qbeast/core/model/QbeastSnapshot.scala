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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

/**
 * A snapshot of the Qbeast table state.
 */
trait QbeastSnapshot {

  def basePath: Path

  /**
   * The current state of the snapshot.
   */
  def isInitial: Boolean

  /**
   * Returns the total number of data files in the snapshot.
   */
  def allFilesCount: Long

  /**
   * Provides the schema of the dataset for this snapshot.
   */
  def schema: StructType

  /**
   * The current table description.
   * @return
   */
  def loadDescription: String

  /**
   * The current table properties of the snapshot.
   * @return
   */
  def loadProperties: Map[String, String]

  /**
   * Load methods
   */

  /**
   * Obtains the latest IndexStatus
   * @return
   *   the index status
   */
  def loadLatestIndexStatus: IndexStatus

  /**
   * Obtains the latest IndexStatus for a given revision
   * @param revisionID
   *   the RevisionID
   * @return
   *   the index status
   */
  def loadIndexStatus(revisionID: RevisionID): IndexStatus

  /**
   * Loads the index files of the lates revision.
   *
   * @return
   *   the index files of the lates revision
   */
  def loadLatestIndexFiles: Dataset[IndexFile]

  /**
   * Loads the index files of the specified revision (revision files).
   *
   * @param revisionID
   *   the revision identifier
   * @return
   *   the index files of the specified revision
   */
  def loadIndexFiles(revisionID: RevisionID): Dataset[IndexFile]

  /**
   * Obtains all Revisions
   * @return
   *   an immutable Seq of Revision
   */
  def loadAllRevisions: IISeq[Revision]

  /**
   * Returns true if a revision with a specific revision identifier exists
   *
   * @param revisionID
   *   the identifier of the revision
   * @return
   *   boolean
   */
  def existsRevision(revisionID: RevisionID): Boolean

  /**
   * Obtains the last Revision available
   * @return
   *   the revision
   */
  def loadLatestRevision: Revision

  /**
   * Obtains the IndexStatus for a given revision
   * @param revisionID
   *   the revision identifier
   * @return
   *   the index status
   */
  def loadRevision(revisionID: RevisionID): Revision

  /**
   * Loads the first revision available at a given timestamp
   * @param timestamp
   *   the timestamp
   * @return
   *   the revision
   */
  def loadRevisionAt(timestamp: Long): Revision

  /**
   * Loads the dataset of qbeast blocks from index files
   * @param indexFile
   *   A dataset of index files
   * @return
   *   the Datasetframe
   */
  def loadDataframeFromIndexFiles(indexFile: Dataset[IndexFile]): DataFrame

  /**
   * Loads the file index
   * @return
   *   the FileIndex
   */
  def loadFileIndex(): FileIndex

}
