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

import io.qbeast.core.index.NormalizedWeight
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
    qbeastSnapshot: QbeastSnapshot,
    revision: Revision,
    announcedSet: Set[CubeId] = Set.empty)
    extends Serializable
    with QbeastStaging {

  /**
   * Dataset of files belonging to the specific revision
   * @return
   *   the dataset of AddFile actions
   */
  def revisionFiles: Dataset[AddFile] =
    // this must be external to the lambda, to avoid SerializationErrors
    qbeastSnapshot.loadRevisionFiles(revision.revisionID).asInstanceOf[Dataset[AddFile]]

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
    // Staging files should not be replicated, and all files belong to the root.
    // All staging blocks have elementCount=0 as no qbeast tags are present.
    val root = revision.createCubeIdRoot()
    SortedMap(
      root -> CubeStatus(root, Weight.MaxValue, Weight.MaxValue.fraction, replicated = false, 0L))
  }

  /**
   * Returns the index state for the given space revision
   * @return
   *   Dataset containing cube information
   */
  def indexCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]
    val dimensionCount = revision.transformations.size
    val desiredCubeSize = revision.desiredCubeSize
    val revisionAddFiles: Dataset[AddFile] = revisionFiles

    import revisionAddFiles.sparkSession.implicits._
    val cubeStatuses = revisionAddFiles
      .flatMap(IndexFiles.fromAddFile(dimensionCount)(_).blocks)
      .groupBy($"cubeId")
      .agg(
        min($"maxWeight.value").as("maxWeightInt"),
        sum($"elementCount").as("elementCount"),
        min(col("replicated")).as("replicated"))
      .withColumn(
        "normalizedWeight",
        when(
          $"maxWeightInt" < lit(Weight.MaxValue.value),
          NormalizedWeight.fromWeightColumn($"maxWeightInt"))
          .otherwise(NormalizedWeight.fromColumns(lit(desiredCubeSize), $"elementCount")))
      .withColumn("maxWeight", struct($"maxWeightInt".as("value")))
      .drop($"maxWeightInt")
      .as[CubeStatus]
      .collect()

    cubeStatuses.foreach(cs => builder += (cs.cubeId -> cs))

    builder.result()
  }

}
