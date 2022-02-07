/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.io.qbeast.core.model

import io.qbeast.core.model.{
  CubeId,
  CubeNormalizedWeights,
  IndexStatus,
  NormalizedWeight,
  Revision,
  RevisionChange,
  TableChanges,
  Weight
}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * Container for the table changes
 *
 * @param revisionChanges the optional revision changes
 * @param indexChanges the index status changes
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
    val announcedOrReplicatedSet: Set[CubeId] =
      if (revisionChanges.isEmpty) {

        supersededIndexStatus.replicatedOrAnnouncedSet ++
          deltaAnnouncedSet ++ deltaReplicatedSet

      } else {
        deltaAnnouncedSet ++ deltaReplicatedSet
      }

    BroadcastedTableChanges(
      revisionChanges.isDefined,
      deltaReplicatedSet,
      SparkSession.active.sparkContext.broadcast(cubeWeights),
      updatedRevision,
      announcedOrReplicatedSet)
  }

}

case class BroadcastedTableChanges(
    isNewRevision: Boolean,
    deltaReplicatedSet: Set[CubeId],
    cubeWeights: Broadcast[Map[CubeId, Weight]],
    updatedRevision: Revision,
    announcedOrReplicatedSet: Set[CubeId])
    extends TableChanges {
  override def cubeWeights(cubeId: CubeId): Option[Weight] = cubeWeights.value.get(cubeId)
}
