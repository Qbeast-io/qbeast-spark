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
package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap

/**
 * Builds the index status from a given snapshot and revision
 *
 * @param qbeastSnapshot
 *   the QbeastSnapshot
 * @param revision
 *   the revision
 * @param announcedSet
 *   the announced set available for the revision
 */
private[delta] class IndexStatusBuilder(
    qbeastSnapshot: DeltaQbeastSnapshot,
    revision: Revision,
    announcedSet: Set[CubeId] = Set.empty)
    extends Serializable
    with StagingUtils {

  /**
   * Dataset of files belonging to the specific revision
   * @return
   *   the dataset of AddFile actions
   */
  def revisionFiles: Dataset[AddFile] =
    // this must be external to the lambda, to avoid SerializationErrors
    qbeastSnapshot.loadRevisionBlocks(revision.revisionID)

  def build(): IndexStatus = {
    val cubeStatus =
      if (isStaging(revision)) stagingCubeStatuses
      else indexCubeStatuses

    val replicatedSet =
      cubeStatus.valuesIterator.filter(_.replicated).map(_.cubeId).toSet

    IndexStatus(
      revision = revision,
      replicatedSet = replicatedSet,
      announcedSet = announcedSet,
      cubesStatuses = cubeStatus)
  }

  def stagingCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val root = revision.createCubeIdRoot()
    val revisionAddFiles = revisionFiles
    import revisionAddFiles.sparkSession.implicits._
    val blocks = revisionAddFiles
      .flatMap(IndexFiles.fromAddFile(root.dimensionCount)(_).blocks)
      .collect()
      .toIndexedSeq
    SortedMap(root -> CubeStatus(root, Weight.MaxValue, Weight.MaxValue.fraction, blocks))
  }

  /**
   * Returns the index state for the given space revision
   *
   * @return
   *   Dataset containing cube information
   */
  def indexCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val dimensionCount = revision.transformations.size
    val desiredCubeSize = revision.desiredCubeSize
    val revisionAddFiles = revisionFiles
    import revisionAddFiles.sparkSession.implicits._
    val items = revisionAddFiles
      .flatMap(IndexFiles.fromAddFile(dimensionCount)(_).blocks)
      .groupBy($"cubeId")
      .agg(
        min($"maxWeight.value").as("maxWeightInt"),
        sum($"elementCount").as("cubeSize"),
        collect_list(struct("*")).as("blocks"))
      .withColumn(
        "normalizedWeight",
        when(
          $"maxWeightInt" < Weight.MaxValueColumn,
          NormalizedWeight.fromWeightColumn($"maxWeightInt"))
          .otherwise(NormalizedWeight.fromColumns(lit(desiredCubeSize), $"cubeSize")))
      .withColumn("maxWeight", struct($"maxWeightInt".as("value")))
      .drop($"maxWeightInt", $"cubeSize")
      .select($"cubeId", struct($"*").as("cubeStatus"))
      .as[(CubeId, CubeStatus)]
      .collect()
    SortedMap(items: _*)
  }

}
