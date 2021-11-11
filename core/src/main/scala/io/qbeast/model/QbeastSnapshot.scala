package io.qbeast.model

import io.qbeast.IISeq

trait QbeastSnapshot {

  /**
   * The current state of the model.
   * @return
   */
  def isInitial: Boolean

  /**
   * Load methods
   */

  /**
   * Obtains the latest IndexStatus for a given QTableID
   * @return the latest IndexStatus for qtable
   */
  def loadLatestIndexStatus: IndexStatus

  /**
   * Obtains the latest IndexStatus for a given RevisionID
   * @param revisionID the RevisionID
   * @return
   */
  def loadIndexStatusAt(revisionID: RevisionID): IndexStatus

  /**
   * Obtain all Revisions for a given QTableID
   * @return an immutable Seq of Revision for qtable
   */
  def loadAllRevisions: IISeq[Revision]

  /**
   * Obtain the last Revisions
   * @return an immutable Seq of Revision for qtable
   */
  def loadLatestRevision: Revision

  /**
   * Obtain the IndexStatus for a given RevisionID
   * @param revisionID the RevisionID
   * @return the IndexStatus for revisionID
   */
  def loadRevision(revisionID: RevisionID): Revision

  /**
   * Loads the most updated revision at a given timestamp
   * @param timestamp the timestamp in Long format
   * @return the latest Revision at a concrete timestamp
   */
  def loadRevisionAt(timestamp: Long): Revision

}
