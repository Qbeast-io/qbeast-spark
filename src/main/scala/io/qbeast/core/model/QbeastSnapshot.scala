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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

/**
 * A snapshot of the Qbeast table state.
 */
trait QbeastSnapshot {

  /**
   * The current state of the snapshot.
   * @return
   */
  def isInitial: Boolean

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
   * Loads the index files of the specified revision.
   *
   * @param revisionId
   *   the revision identifier
   * @return
   *   the index files of the specified revision
   */
  def loadIndexFiles(revisionId: RevisionID): Dataset[IndexFile]

  /**
   * Obtains all Revisions
   * @return
   *   an immutable Seq of Revision
   */
  def loadAllRevisions: IISeq[Revision]

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

  def loadDataframeFromData(indexFile: Dataset[IndexFile]): DataFrame

}
