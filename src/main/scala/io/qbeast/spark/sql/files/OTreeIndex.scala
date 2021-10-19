/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.files

import io.qbeast.spark.index.{CubeId, Weight}
import io.qbeast.spark.model.{Point, QuerySpace, QuerySpaceFromTo, RangeValues}
import io.qbeast.spark.sql.qbeast
import io.qbeast.spark.sql.utils.State
import io.qbeast.spark.sql.utils.TagUtils
import io.qbeast.spark.sql.utils.QbeastExpressionUtils._
import org.apache.spark.sql.catalyst.expressions._
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

  private val qbeastSnapshot = qbeast.QbeastSnapshot(snapshot)

  private val indexedCols = qbeastSnapshot.indexedCols

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val (qbeastDataFilters, tahoeDataFilters) = extractDataFilters(dataFilters, indexedCols)
    val tahoeMatchingFiles = index.matchingFiles(partitionFilters, tahoeDataFilters)

    val (minWeight, maxWeight) = extractWeightRange(qbeastDataFilters)
    val (from, to) = extractQueryRange(qbeastDataFilters, indexedCols)
    val files = sample(minWeight, maxWeight, from, to, tahoeMatchingFiles)

    files
  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  /**
   * Given a From-To range, initialize QuerySpace
   * and find the files that satisfy the predicates
   */

  def sample(
      fromPrecision: Weight,
      toPrecision: Weight,
      originalFrom: Point,
      originalTo: Point,
      files: Seq[AddFile]): Seq[AddFile] = {

    val samplingRange = RangeValues(fromPrecision, toPrecision) match {
      case range if range.to == Weight.MinValue => return List()
      case range if range.from > range.to => return List()
      case range => range
    }

    val filesVector = files.toVector
    qbeastSnapshot.spaceRevisionsMap.values
      .flatMap(spaceRevision => {
        val querySpace = QuerySpaceFromTo(originalFrom, originalTo, spaceRevision)

        val revisionTimestamp = spaceRevision.timestamp
        val filesRevision =
          filesVector.filter(_.tags(TagUtils.space) == spaceRevision.timestamp.toString)
        val cubeWeights = qbeastSnapshot.cubeWeights(revisionTimestamp)
        val replicatedSet = qbeastSnapshot.replicatedSet(revisionTimestamp)

        findSampleFiles(
          querySpace,
          samplingRange,
          CubeId.root(qbeastSnapshot.dimensionCount),
          filesRevision,
          cubeWeights,
          replicatedSet)

      })
      .toSeq

  }

  /**
   * Finds the files to retrieve the query sample.
   *
   * @param space the query space
   * @param precision the sample precision range
   * @param startCube the start cube
   * @param files the data files
   * @param cubeWeights the cube weights
   * @return the files with sample data
   */
  def findSampleFiles(
      space: QuerySpace,
      precision: RangeValues,
      startCube: CubeId,
      files: Vector[AddFile],
      cubeWeights: Map[CubeId, Weight],
      replicatedSet: Set[CubeId]): Vector[AddFile] = {

    def doFindSampleFiles(cube: CubeId): Vector[AddFile] = {
      cubeWeights.get(cube) match {
        case Some(cubeWeight) if precision.to < cubeWeight =>
          val cubeString = cube.string
          files.filter(_.tags(TagUtils.cube) == cubeString)
        case Some(cubeWeight) =>
          val cubeString = cube.string
          val childFiles = cube.children
            .filter(space.intersectsWith)
            .flatMap(doFindSampleFiles)
          if (!replicatedSet.contains(cube) && precision.from < cubeWeight) {
            val cubeFiles = files.filter(_.tags(TagUtils.cube) == cubeString)
            if (childFiles.nonEmpty) {
              cubeFiles.filterNot(_.tags(TagUtils.state) == State.ANNOUNCED) ++ childFiles
            } else {
              cubeFiles
            }
          } else {
            childFiles.toVector
          }
        case None => Vector.empty
      }
    }

    doFindSampleFiles(startCube)
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes
}
