/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap
import scala.collection.mutable

/**
 * Builds the index status from a given snapshot and revision
 * @param qbeastSnapshot the QbeastSnapshot
 * @param revision the revision
 * @param announcedSet the announced set available for the revision
 * @param replicatedSet the replicated set available for the revision
 */
private[delta] class IndexStatusBuilder(
    qbeastSnapshot: DeltaQbeastSnapshot,
    revision: Revision,
    replicatedSet: ReplicatedSet,
    announcedSet: Set[CubeId] = Set.empty)
    extends Serializable
    with StagingUtils {

  /**
   * Dataset of files belonging to the specific revision
   * @return the dataset of AddFile actions
   */
  def revisionFiles: Dataset[AddFile] =
    // this must be external to the lambda, to avoid SerializationErrors
    qbeastSnapshot.loadRevisionBlocks(revision.revisionID)

  def build(): IndexStatus = {
    val cubeStatus =
      if (isStaging(revision)) stagingCubeStatuses
      else indexCubeStatuses

    IndexStatus(
      revision = revision,
      replicatedSet = replicatedSet,
      announcedSet = announcedSet,
      cubesStatuses = cubeStatus)
  }

  def stagingCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val root = revision.createCubeIdRoot()
    val dimensionCount = revision.transformations.length
    val maxWeight = Weight.MaxValue
    val blocks = revisionFiles
      .collect()
      .iterator
      .map(IndexFiles.fromAddFile(dimensionCount))
      .flatMap(_.blocks)
      .toIndexedSeq
    SortedMap(root -> CubeStatus(root, maxWeight, maxWeight.fraction, blocks))
  }

  /**
   * Returns the index state for the given space revision
   * @return Dataset containing cube informatio
   */
  def indexCubeStatuses: SortedMap[CubeId, CubeStatus] = {
    val builders = mutable.Map.empty[CubeId, CubeStatusBuilder]
    val dimensionCount = revision.transformations.length
    val desiredCubeSize = revision.desiredCubeSize
    revisionFiles
      .collect()
      .iterator
      .map(IndexFiles.fromAddFile(dimensionCount))
      .flatMap(_.blocks)
      .foreach { block =>
        val builder = builders.getOrElseUpdate(
          block.cubeId,
          new CubeStatusBuilder(block.cubeId, desiredCubeSize))
        builder.addBlock(block)
      }
    val statuses = SortedMap.newBuilder[CubeId, CubeStatus]
    builders.foreach { case (cubeId, builder) => statuses += cubeId -> builder.result() }
    statuses.result()
  }

}
