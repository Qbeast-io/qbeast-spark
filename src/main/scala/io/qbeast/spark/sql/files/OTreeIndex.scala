/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.files

import io.qbeast.spark.index.{CubeId, Weight}
import io.qbeast.spark.model.{Point, QuerySpace, QuerySpaceFromTo, RangeValues}
import io.qbeast.spark.sql.qbeast
import io.qbeast.spark.sql.utils.State
import io.qbeast.spark.sql.utils.TagUtils
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

  private val qbeastSnapshot = qbeast.QbeastSnapshot(snapshot)

  /**
   * Analyzes the data filters from the query
   * @param dataFilters filters passed to the relation
   * @return min max weight (default Integer.MINVALUE, Integer.MAXVALUE)
   */
  private def extractDataFilters(
      dataFilters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    dataFilters.partition(e => e.children.head.prettyName.equals("qbeast_hash"))
  }

  // TODO this ugly implementation is to test if it works

  private def hasColumnReferences(attr: Expression, columnName: String): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    attr.references.forall(r => nameEquality(r.name, columnName))
  }

  private def extractRangeFilters(
      dataFilters: Seq[Expression],
      dimensionColumns: Seq[String]): (Point, Point) = {

    val fromTo = dimensionColumns.map(columnName => {

      val columnFilters = dataFilters
        .filter(hasColumnReferences(_, columnName))

      val from = columnFilters
        .collectFirst {
          case expressions.GreaterThanOrEqual(_, Literal(m, _)) => m
          case expressions.EqualTo(_, Literal(m, _)) => m
        }
        .getOrElse(Int.MinValue)
        .asInstanceOf[Int]
        .doubleValue()

      val to = columnFilters
        .collectFirst { case expressions.LessThan(_, Literal(m, _)) => m }
        .getOrElse(Int.MaxValue)
        .asInstanceOf[Int]
        .doubleValue()

      (from, to)

    })

    val from = fromTo.map(_._1)
    val to = fromTo.map(_._2)
    (Point(from.toVector), Point(to.toVector))
  }

  private def extractWeightRange(filters: Seq[Expression]): (Weight, Weight) = {
    val min = filters
      .collectFirst { case expressions.GreaterThanOrEqual(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .getOrElse(Int.MinValue)

    val max = filters
      .collectFirst { case expressions.LessThan(_, Literal(m, IntegerType)) =>
        m.asInstanceOf[Int]
      }
      .getOrElse(Int.MaxValue)

    (Weight(min), Weight(max))
  }

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val (qbeastDataFilters, tahoeDataFilters) = extractDataFilters(dataFilters)
    val tahoeMatchingFiles = index.matchingFiles(partitionFilters, tahoeDataFilters)

    val (minWeight, maxWeight) = extractWeightRange(qbeastDataFilters)
    val files = sample(minWeight, maxWeight, tahoeMatchingFiles, dataFilters)

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
      files: Seq[AddFile],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val samplingRange = RangeValues(fromPrecision, toPrecision) match {
      case range if range.to == Weight.MinValue => return List()
      case range if range.from > range.to => return List()
      case range => range
    }

    val (originalFrom, originalTo) = extractRangeFilters(dataFilters, qbeastSnapshot.indexedCols)

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
