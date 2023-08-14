/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import io.qbeast.spark.utils.TagUtils
import io.qbeast.spark.utils.State

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
      .map(IndexStatusBuilder.addFileToIndexFile(dimensionCount))
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
      .map(IndexStatusBuilder.addFileToIndexFile(dimensionCount))
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

private object IndexStatusBuilder {

  private def addFileToIndexFile(dimensionCount: Int)(addFile: AddFile): IndexFile = {
    val file = File(addFile.path, addFile.size, addFile.modificationTime)
    val revisionId = addFile.getTag(TagUtils.revision).map(_.toLong).getOrElse(0L)
    val to = addFile.getTag(TagUtils.elementCount).map(_.toLong).getOrElse(0L)
    val range = RowRange(0, to)
    val cubeId = CubeId(dimensionCount, addFile.getTag(TagUtils.cube).getOrElse(""))
    val state = addFile.getTag(TagUtils.state).getOrElse(State.FLOODED)
    val minWeight =
      addFile.getTag(TagUtils.minWeight).map(_.toInt).map(Weight.apply).getOrElse(Weight.MinValue)
    val maxWeight =
      addFile.getTag(TagUtils.maxWeight).map(_.toInt).map(Weight.apply).getOrElse(Weight.MaxValue)
    val block = Block(file, range, cubeId, state, minWeight, maxWeight)
    IndexFile(file, revisionId, Seq(block))
  }

}
