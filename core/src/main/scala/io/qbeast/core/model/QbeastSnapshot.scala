package io.qbeast.core.model

import io.qbeast.IISeq

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
  def loadLatestIndexFiles: IISeq[IndexFile]

  /**
   * Loads the index files of the specified revision.
   *
   * @param revisionId
   *   the revision identitifier
   * @return
   *   the index files of the specified revision
   */
  def loadIndexFiles(revisionId: RevisionID): IISeq[IndexFile]

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

}
