package io.qbeast.core.model

import io.qbeast.IISeq

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
   * @return the index status
   */
  def loadLatestIndexStatus: IndexStatus

  /**
   * Obtains the latest IndexStatus for a given revision
   * @param revisionID the RevisionID
   * @return the index status
   */
  def loadIndexStatus(revisionID: RevisionID): IndexStatus

  /**
   * Obtains all Revisions
   * @return an immutable Seq of Revision
   */
  def loadAllRevisions: IISeq[Revision]

  /**
   * Obtains the last Revision available
   * @return the revision
   */
  def loadLatestRevision: Revision

  /**
   * Obtains the IndexStatus for a given revision
   * @param revisionID the revision identifier
   * @return the index status
   */
  def loadRevision(revisionID: RevisionID): Revision

  /**
   * Loads the first revision available at a given timestamp
   * @param timestamp the timestamp
   * @return the revision
   */
  def loadRevisionAt(timestamp: Long): Revision

}
