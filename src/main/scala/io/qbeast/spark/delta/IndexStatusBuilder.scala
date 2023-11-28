/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap
import scala.collection.JavaConverters._

/**
 * Builds the index status from a given snapshot and revision
 *
 * @param qbeastSnapshot
 *   the QbeastSnapshot
 * @param revision
 *   the revision
 * @param announcedSet
 *   the announced set available for the revision
 * @param replicatedSet
 *   the replicated set available for the revision
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
    val blocks = revisionFiles
      .toLocalIterator()
      .asScala
      .map(IndexFiles.fromAddFile(root.dimensionCount))
      .flatMap(_.blocks)
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
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]
    val dimensionCount = revision.transformations.size
    val desiredCubeSize = revision.desiredCubeSize
    revisionFiles
      .toLocalIterator()
      .asScala
      .map(IndexFiles.fromAddFile(dimensionCount))
      .flatMap(_.blocks)
      .toSeq
      .groupBy(_.cubeId)
      .iterator
      .map { case (cubeId, blocks) =>
        val maxWeight = blocks.map(_.maxWeight).min
        val cubeSize = blocks.map(_.elementCount).sum
        val normalizedWeight =
          if (maxWeight < Weight.MaxValue) maxWeight.fraction
          else NormalizedWeight(desiredCubeSize, cubeSize)
        val status = CubeStatus(cubeId, maxWeight, normalizedWeight, blocks.toIndexedSeq)
        (cubeId, status)
      }
      .foreach(builder += _)
    builder.result()
  }

}
