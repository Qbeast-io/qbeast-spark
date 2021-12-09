/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.spark.sql.catalyst.expressions.Expression
import io.qbeast.spark.utils.QbeastExpressionUtils.{
  extractDataFilters,
  extractQuerySpace,
  extractWeightRange
}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 */
case class OTreeIndex(index: TahoeLogFileIndex)
    extends TahoeFileIndex(index.spark, index.deltaLog, index.path) {

  /**
   * Snapshot to analyze
   * @return the snapshot
   */
  protected def snapshot: Snapshot = index.getSnapshot

  private def qbeastSnapshot = DeltaQbeastSnapshot(snapshot)

  private def latestRevision = qbeastSnapshot.loadLatestRevision

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val (qbeastDataFilters, tahoeDataFilters) =
      extractDataFilters(dataFilters ++ partitionFilters, latestRevision)
    val weightRange = extractWeightRange(qbeastDataFilters)
    val tahoeMatchingFiles = index.matchingFiles(partitionFilters, tahoeDataFilters)
    sample(weightRange, qbeastDataFilters, tahoeMatchingFiles)

  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  /**
   * Given a set of data filters, returns the files that match the filters
   * @param dataFilters the From-To weight range
   * @param files the files available to be sampled
   * return the sequence of files to read
   */

  def sample(
      weightRange: WeightRange,
      dataFilters: Seq[Expression],
      files: Seq[AddFile]): Seq[AddFile] = {

    if (weightRange.to == Weight.MinValue || weightRange.from > weightRange.to) return List.empty

    // For each of the available revisions, sample the files required
    // through the revision collected from the Delta Snapshot
    qbeastSnapshot.loadAllRevisions
      .flatMap(revision => {

        val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
        val dimensionCount = revision.columnTransformers.length
        val querySpace = extractQuerySpace(dataFilters, revision)

        val cubeStatus = indexStatus.cubesStatuses
        val replicatedSet = indexStatus.replicatedSet

        findSampleFiles(
          querySpace,
          weightRange,
          CubeId.root(dimensionCount),
          cubeStatus,
          replicatedSet,
          files)
      })

  }

  /**
   * Finds the files to retrieve the query sample.
   *
   * @param space the query space
   * @param weightRange the weight range
   * @param startCube the start cube
   * @param cubesStatuses the cube weights and files
   * @param replicatedSet the replicated set
   * @param previouslyMatchedFiles the files that have been matched
   * @return the files with sample data
   */
  def findSampleFiles(
      space: QuerySpace,
      weightRange: WeightRange,
      startCube: CubeId,
      cubesStatuses: Map[CubeId, CubeStatus],
      replicatedSet: Set[CubeId],
      previouslyMatchedFiles: Seq[AddFile]): IISeq[AddFile] = {
    val fileMap = previouslyMatchedFiles.map(a => (a.path, a)).toMap
    def doFindSampleFiles(cube: CubeId): IISeq[AddFile] = {
      cubesStatuses.get(cube) match {
        case Some(CubeStatus(maxWeight, _, files)) if weightRange.to < maxWeight =>
          files.flatMap(fileMap.get)
        case Some(CubeStatus(maxWeight, _, files)) =>
          val childFiles: Iterator[AddFile] = cube.children
            .filter(space.intersectsWith)
            .flatMap(doFindSampleFiles)
          if (!replicatedSet.contains(cube) && weightRange.from < maxWeight) {
            val cubeFiles = files.flatMap(fileMap.get)
            if (childFiles.nonEmpty) {
              cubeFiles.filterNot(_.tags(TagUtils.state) == State.ANNOUNCED) ++ childFiles
            } else {
              cubeFiles
            }
          } else {
            childFiles.toVector
          }
        case None =>
          // TODO how we manage non-existing/empty cubes if the children are present?
          Vector.empty
      }
    }

    doFindSampleFiles(startCube)
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes
}
