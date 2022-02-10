/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.spark.utils.State
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

    val cubeStates = replicatedSet.map(id => id -> State.REPLICATED) ++
      (announcedSet -- replicatedSet).map(id => id -> State.ANNOUNCED)

    BroadcastedTableChanges(
      isNewRevision = revisionChanges.isDefined,
      isOptimizeOperation = deltaReplicatedSet.nonEmpty,
      updatedRevision = updatedRevision,
      deltaReplicatedSet = deltaReplicatedSet,
      announcedOrReplicatedSet = announcedSet ++ replicatedSet,
      cubeStates = SparkSession.active.sparkContext.broadcast(cubeStates.toMap),
      cubeWeights = SparkSession.active.sparkContext.broadcast(cubeWeights))
  }

}

case class BroadcastedTableChanges(
    isNewRevision: Boolean,
    isOptimizeOperation: Boolean,
    updatedRevision: Revision,
    deltaReplicatedSet: Set[CubeId],
    announcedOrReplicatedSet: Set[CubeId],
    cubeStates: Broadcast[Map[CubeId, String]],
    cubeWeights: Broadcast[Map[CubeId, Weight]])
    extends TableChanges {

  override def cubeWeights(cubeId: CubeId): Option[Weight] = cubeWeights.value.get(cubeId)

  override def cubeState(cubeId: CubeId): Option[String] = cubeStates.value.get(cubeId)

}
