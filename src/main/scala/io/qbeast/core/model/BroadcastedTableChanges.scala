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

import io.qbeast.spark.model.CubeState
import io.qbeast.spark.model.CubeState.CubeStateValue
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * Container for the table changes
 */

object BroadcastedTableChanges {

  def apply(
      revisionChanges: Option[RevisionChange],
      supersededIndexStatus: IndexStatus,
      deltaNormalizedCubeWeights: Map[CubeId, Weight],
      newBlocksElementCount: Map[CubeId, Long],
      deltaReplicatedSet: Set[CubeId] = Set.empty,
      deltaAnnouncedSet: Set[CubeId] = Set.empty): TableChanges = {
    val sparkContext = SparkSession.active.sparkContext

    BroadcastedTableChanges.create(
      supersededIndexStatus,
      sparkContext.broadcast(deltaNormalizedCubeWeights),
      sparkContext.broadcast(newBlocksElementCount),
      deltaReplicatedSet,
      deltaAnnouncedSet,
      revisionChanges)

  }

  def create(
      supersededIndexStatus: IndexStatus,
      deltaNormalizedCubeWeightsBroadcast: Broadcast[Map[CubeId, Weight]],
      newBlocksElementCountBroadcast: Broadcast[Map[CubeId, Long]],
      deltaReplicatedSet: Set[CubeId] ,
      deltaAnnouncedSet: Set[CubeId],
      revisionChanges: Option[RevisionChange]): TableChanges = {

    val updatedRevision = revisionChanges match {
      case Some(newRev) => newRev.createNewRevision
      case None => supersededIndexStatus.revision
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
      isOptimizeOperation = deltaReplicatedSet.nonEmpty,
      updatedRevision = updatedRevision,
      deltaReplicatedSet = deltaReplicatedSet,
      announcedOrReplicatedSet = announcedSet ++ replicatedSet,
      cubeWeightsBroadcast = deltaNormalizedCubeWeightsBroadcast,
      newBlockStatsBroadcast = newBlocksElementCountBroadcast)
  }

}

case class BroadcastedTableChanges private[model] (
    isNewRevision: Boolean,
    isOptimizeOperation: Boolean,
    updatedRevision: Revision,
    deltaReplicatedSet: Set[CubeId],
    announcedOrReplicatedSet: Set[CubeId],
    // this contains an entry for each cube in the index
    cubeWeightsBroadcast: Broadcast[Map[CubeId, Weight]],
    // this map contains an entry for each new block added in this write operation.
    newBlockStatsBroadcast: Broadcast[Map[CubeId, Long]])
    extends TableChanges {

  override def cubeWeight(cubeId: CubeId): Option[Weight] =
    cubeWeightsBroadcast.value.get(cubeId)

  override def cubeState(cubeId: CubeId): CubeStateValue = {
    if (announcedOrReplicatedSet.contains(cubeId)) {
      CubeState.ANNOUNCED
    } else {
      CubeState.FLOODED
    }
  }

  override def deltaBlockElementCount: Map[CubeId, Long] =
    newBlockStatsBroadcast.value

}
