/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * Container for the table changes
 */

object BroadcastedTableChanges {

  def apply(
      revisionChanges: Option[RevisionChange],
      supersededIndexStatus: IndexStatus,
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
      deltaReplicatedSet: Set[CubeId] = Set.empty,
      deltaAnnouncedSet: Set[CubeId] = Set.empty): TableChanges = {

    val updatedRevision = revisionChanges match {
      case Some(newRev) => newRev.createNewRevision
      case None => supersededIndexStatus.revision
    }
    val cubeWeights = if (revisionChanges.isEmpty) {

      CubeNormalizedWeights.mergeNormalizedWeights(
        supersededIndexStatus.cubeNormalizedWeights,
        deltaNormalizedCubeWeights)
    } else {
      CubeNormalizedWeights.mergeNormalizedWeights(Map.empty, deltaNormalizedCubeWeights)
    }

    val replicatedSet = if (revisionChanges.isEmpty) {

      supersededIndexStatus.replicatedSet ++ deltaReplicatedSet

    } else {
      deltaReplicatedSet
    }

    val announcedSet = if (revisionChanges.isEmpty) {

      supersededIndexStatus.announcedSet ++ deltaAnnouncedSet

    } else {
      deltaAnnouncedSet
    }

    BroadcastedTableChanges(
      isNewRevision = revisionChanges.isDefined,
      deltaReplicatedSet = deltaReplicatedSet,
      cubeWeights = SparkSession.active.sparkContext.broadcast(cubeWeights),
      updatedRevision = updatedRevision,
      announcedSet = announcedSet,
      replicatedSet = replicatedSet)
  }

}

case class BroadcastedTableChanges(
    isNewRevision: Boolean,
    deltaReplicatedSet: Set[CubeId],
    cubeWeights: Broadcast[Map[CubeId, Weight]],
    updatedRevision: Revision,
    announcedSet: Set[CubeId],
    replicatedSet: Set[CubeId])
    extends TableChanges {

  override val announcedOrReplicatedSet: Set[CubeId] = announcedSet ++ replicatedSet

  override def cubeWeights(cubeId: CubeId): Option[Weight] = cubeWeights.value.get(cubeId)
}
