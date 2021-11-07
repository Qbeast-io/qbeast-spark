/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model._
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.types.IntegerType

/**
 * Integer values of range [from, to)
 * @param from from value
 * @param to to value
 */
case class RangeValues(from: Weight, to: Weight)

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

  private val qbeastSnapshot = QbeastSnapshot(snapshot)

  /**
   * Analyzes the data filters from the query
   * @param dataFilters filters passed to the relation
   * @return min max weight (default Integer.MINVALUE, Integer.MAXVALUE)
   */
  private def extractDataFilters(
      dataFilters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    dataFilters.partition(e => e.children.head.prettyName.equals("qbeast_hash"))
  }

  private def extractWeightRange(filters: Seq[Expression]): (Weight, Weight) = {
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

    (Weight(min), Weight(max))
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val (qbeastDataFilters, tahoeDataFilters) = extractDataFilters(dataFilters)
    val tahoeMatchingFiles = index.matchingFiles(partitionFilters, tahoeDataFilters)

    val (minWeight, maxWeight) = extractWeightRange(qbeastDataFilters)
    val files = sample(minWeight, maxWeight, tahoeMatchingFiles)

    files
  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  /**
   * Given a From-To range, initialize QuerySpace
   * and find the files that satisfy the predicates
   */

  def sample(fromPrecision: Weight, toPrecision: Weight, files: Seq[AddFile]): Seq[AddFile] = {

    val samplingRange = RangeValues(fromPrecision, toPrecision) match {
      case range if range.to == Weight.MinValue => return List()
      case range if range.from > range.to => return List()
      case range => range
    }

    val filesVector = files.toVector
    qbeastSnapshot.revisions
      .flatMap(revision => {

        val revisionData = qbeastSnapshot.getIndexStatus(revision.revisionID)
        val dimensionCount = revision.columnTransformers.length

        val originalFrom = Point(Vector.fill(dimensionCount)(Int.MinValue.doubleValue()))
        val originalTo = Point(Vector.fill(dimensionCount)(Int.MaxValue.doubleValue()))
        val querySpace = QuerySpaceFromTo(originalFrom, originalTo, revision)

        val cubeWeights = revisionData.cubeWeights
        val replicatedSet = revisionData.replicatedSet
        val filesRevision =
          filesVector.filter(_.tags(TagUtils.revision) == revision.revisionID.toString)

        findSampleFiles(
          querySpace,
          samplingRange,
          CubeId.root(dimensionCount),
          filesRevision,
          cubeWeights,
          replicatedSet)
      })

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
