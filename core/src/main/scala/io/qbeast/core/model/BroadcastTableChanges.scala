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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * Container for the table changes
 */
case class BroadcastTableChanges private[model] (
    isNewRevision: Boolean,
    updatedRevision: Revision,
    cubeWeightsBroadcast: Broadcast[Map[CubeId, Weight]],
    inputBlockElementCountsBroadcast: Broadcast[Map[CubeId, Long]])
    extends TableChanges {

  override def cubeWeight(cubeId: CubeId): Option[Weight] =
    cubeWeightsBroadcast.value.get(cubeId)

  override def inputBlockElementCounts: Map[CubeId, Long] =
    inputBlockElementCountsBroadcast.value

}

object BroadcastTableChanges {

  def apply(
      revisionChanges: Option[RevisionChange],
      existingIndexStatus: IndexStatus,
      updatedCubeWeights: Map[CubeId, Weight],
      inputBlockElementCounts: Map[CubeId, Long]): TableChanges = {
    val sparkContext = SparkSession.active.sparkContext
    val (updatedRevision, isNewRevision) = revisionChanges match {
      case Some(newRev) => (newRev.createNewRevision, true)
      case None => (existingIndexStatus.revision, false)
    }
    val cubeWeightsBroadcast = sparkContext.broadcast(updatedCubeWeights)
    val inputBlockElementCountsBroadcast = sparkContext.broadcast(inputBlockElementCounts)
    BroadcastTableChanges(
      isNewRevision = isNewRevision,
      updatedRevision = updatedRevision,
      cubeWeightsBroadcast = cubeWeightsBroadcast,
      inputBlockElementCountsBroadcast = inputBlockElementCountsBroadcast)
  }

}
