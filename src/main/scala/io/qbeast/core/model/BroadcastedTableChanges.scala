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
    // scalastyle:off println
    print(s"Root deltaNormalizedWeight: ${deltaNormalizedCubeWeights(
      supersededIndexStatus.revision.createCubeIdRoot())}")
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

    val announcedSet = if (revisionChanges.isEmpty) {
      supersededIndexStatus.announcedSet ++ deltaAnnouncedSet
    } else {
      deltaAnnouncedSet
    }
    val replicatedSet = if (revisionChanges.isEmpty) {
      supersededIndexStatus.replicatedSet ++ deltaReplicatedSet
    } else {
      deltaReplicatedSet
    }

    val cubeStates = replicatedSet.map(id => id -> State.REPLICATED) ++
      (announcedSet -- replicatedSet).map(id => id -> State.ANNOUNCED)

    // Skip tree compression entirely during optimization
    val isReplication = deltaReplicatedSet.nonEmpty
    val compressionMap = if (isReplication) {
      Map.empty[CubeId, CubeId]
    } else {
      CubeNormalizedWeights.treeCompression(
        supersededIndexStatus.cubeNormalizedWeights,
        cubeWeights,
        cubeStates.map(_._1),
        updatedRevision.desiredCubeSize)
    }

    BroadcastedTableChanges(
      isNewRevision = revisionChanges.isDefined,
      isOptimizeOperation = deltaReplicatedSet.nonEmpty,
      updatedRevision = updatedRevision,
      compressionMap = compressionMap,
      deltaReplicatedSet = deltaReplicatedSet,
      announcedOrReplicatedSet = announcedSet ++ replicatedSet,
      cubeStates = SparkSession.active.sparkContext.broadcast(cubeStates.toMap),
      cubeWeights = SparkSession.active.sparkContext.broadcast(
        cubeWeights
          .mapValues(NormalizedWeight.toWeight)
          .map(identity)))
  }

}

case class BroadcastedTableChanges(
    isNewRevision: Boolean,
    isOptimizeOperation: Boolean,
    updatedRevision: Revision,
    compressionMap: Map[CubeId, CubeId],
    deltaReplicatedSet: Set[CubeId],
    announcedOrReplicatedSet: Set[CubeId],
    cubeStates: Broadcast[Map[CubeId, String]],
    cubeWeights: Broadcast[Map[CubeId, Weight]])
    extends TableChanges {

  override def cubeWeights(cubeId: CubeId): Option[Weight] = cubeWeights.value.get(cubeId)

  override def cubeState(cubeId: CubeId): Option[String] = cubeStates.value.get(cubeId)

}
