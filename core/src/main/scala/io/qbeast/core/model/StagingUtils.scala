/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.core.transform.EmptyTransformer

trait StagingUtils {

  /**
   * RevisionID for the Staging Revision
   */
  protected val stagingID: RevisionID = 0L

  protected def isStaging(revisionID: RevisionID): Boolean = {
    revisionID == stagingID
  }

  protected def isStaging(revision: Revision): Boolean = {
    isStaging(revision.revisionID) &&
    revision.columnTransformers.forall {
      case _: EmptyTransformer => true
      case _ => false
    }
  }

  /**
   * Initialize Revision for table conversion. The RevisionID for a converted table is 0.
   * EmptyTransformers and EmptyTransformations are used. This Revision should always be
   * superseded.
   */
  protected def stagingRevision(
      tableID: QTableID,
      desiredCubeSize: Int,
      desiredFileSize: Long,
      columnsToIndex: Seq[String]): Revision = {
    val emptyTransformers = columnsToIndex.map(s => EmptyTransformer(s)).toIndexedSeq
    val emptyTransformations = emptyTransformers.map(_.makeTransformation(r => r))

    Revision(
      stagingID,
      System.currentTimeMillis(),
      tableID,
      desiredCubeSize,
      desiredFileSize,
      emptyTransformers,
      emptyTransformations)
  }

}
