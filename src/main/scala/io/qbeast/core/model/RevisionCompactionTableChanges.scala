/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

object RevisionCompactionTableChanges {

  def apply(tableChanges: TableChanges, compactedRevisionIDs: Seq[RevisionID]): TableChanges = {
    val updatedRevision = if (tableChanges.isNewRevision) {
      val revisionIDToUse = tableChanges.updatedRevision.revisionID - 1
      tableChanges.updatedRevision.copy(revisionID = revisionIDToUse)
    } else tableChanges.updatedRevision

    RevisionCompactionTableChanges(
      tableChanges.isNewRevision,
      isOptimizeOperation = false,
      updatedRevision,
      compactedRevisionIDs)
  }

}

case class RevisionCompactionTableChanges(
    isNewRevision: Boolean,
    isOptimizeOperation: Boolean,
    updatedRevision: Revision,
    compactedRevisionIDs: Seq[RevisionID])
    extends TableChanges {
  override val deltaReplicatedSet: Set[CubeId] = Set.empty
  override val announcedOrReplicatedSet: Set[CubeId] = Set.empty

  override def cubeState(cubeId: CubeId): Option[String] = None

  override def cubeWeights(cubeId: CubeId): Option[Weight] = None
}
