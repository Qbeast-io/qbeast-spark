/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.types.IntegerType

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

  /**
   * Analyzes the data filters from the query
   * @param dataFilters filters passed to the relation
   * @return min max weight (default Integer.MINVALUE, Integer.MAXVALUE)
   */
  private def extractDataFilters(
      dataFilters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    dataFilters.partition(e => e.children.head.prettyName.equals("qbeast_hash"))
  }

  private def extractWeightRange(filters: Seq[Expression]): WeightRange = {
    val min = filters
      .collect { case expressions.GreaterThanOrEqual(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .reduceOption(_ min _)
      .getOrElse(Int.MinValue)

    val max = filters
      .collect { case expressions.LessThan(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .reduceOption(_ max _)
      .getOrElse(Int.MaxValue)

    WeightRange(Weight(min), Weight(max))
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val (qbeastDataFilters, tahoeDataFilters) = extractDataFilters(dataFilters)
    val tahoeMatchingFiles = index.matchingFiles(partitionFilters, tahoeDataFilters)

    val weightRange = extractWeightRange(qbeastDataFilters)
    val files = sample(weightRange, tahoeMatchingFiles)

    files
  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  /**
   * Given a From-To weight range, initialize QuerySpace
   * and find the files that satisfy the predicates
   * @param weightRange the From-To weight range
   * @param files the files available to be sampled
   * return the sequence of files to read
   */

  def sample(weightRange: WeightRange, files: Seq[AddFile]): Seq[AddFile] = {

    if (weightRange.to == Weight.MinValue || weightRange.from > weightRange.to) return List.empty

    qbeastSnapshot.loadAllRevisions
      .flatMap(revision => {

        val revisionData = qbeastSnapshot.loadIndexStatus(revision.revisionID)
        val dimensionCount = revision.columnTransformers.length
        val querySpace = AllSpace(dimensionCount)

        val cubeStatus = revisionData.cubesStatuses
        val replicatedSet = revisionData.replicatedSet

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

  // TODO: turn this into a iterative function
  //  and substitute the MAP for a PatriciaTree solution
  def findSampleFiles(
      space: QuerySpace,
      weightRange: WeightRange,
      startCube: CubeId,
      cubesStatuses: Map[CubeId, CubeStatus],
      replicatedSet: Set[CubeId],
      previouslyMatchedFiles: Seq[AddFile]): IISeq[AddFile] = {

    val keys = cubesStatuses.keys.map(_.string).toVector
    def haveImissingDescendands(cube: CubeId): Boolean = {
      val c = cube.string
      keys.count(cubeString => cubeString.startsWith(c)) > 1
    }
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
          if (haveImissingDescendands(cube)) {
            cube.children
              .filter(space.intersectsWith)
              .flatMap(doFindSampleFiles)
              .toVector
          } else {
            Vector.empty
          }
      }
    }

    doFindSampleFiles(startCube)
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes
}
