/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.files

import io.qbeast.spark.index.{CubeId, Weight}
import io.qbeast.spark.model.{Point, QuerySpace, QuerySpaceFromTo, RangeValues}
import io.qbeast.spark.sql.qbeast
import io.qbeast.spark.sql.utils.State.ANNOUNCED
import io.qbeast.spark.sql.utils.TagUtils._
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
case class OTreeIndex(index: TahoeLogFileIndex, desiredCubeSize: Int)
    extends TahoeFileIndex(index.spark, index.deltaLog, index.path) {

  /**
   * Snapshot to analyze
   * @return the snapshot
   */
  protected def snapshot: Snapshot = index.getSnapshot

  private val qbeastSnapshot = qbeast.QbeastSnapshot(snapshot, desiredCubeSize)

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

    val (qbeastDataFilters, _) = extractDataFilters(dataFilters)
    val (minWeight, maxWeight) = extractWeightRange(qbeastDataFilters)
    val files = sample(minWeight, maxWeight, qbeastSnapshot.allFiles)

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

    val originalFrom = Point(
      Vector.fill(qbeastSnapshot.dimensionCount)(Int.MinValue.doubleValue()))
    val originalTo = Point(Vector.fill(qbeastSnapshot.dimensionCount)(Int.MaxValue.doubleValue()))

    val filesVector = files.toVector
    qbeastSnapshot.spaceRevisions
      .flatMap(spaceRevision => {
        val querySpace = QuerySpaceFromTo(originalFrom, originalTo, spaceRevision)

        val filesRevision = filesVector.filter(_.tags(spaceTag).equals(spaceRevision.toString))
        val cubeWeights = qbeastSnapshot.cubeWeights(spaceRevision)
        val replicatedSet = qbeastSnapshot.replicatedSet(spaceRevision)

        findSampleFiles(
          querySpace,
          samplingRange,
          CubeId.root(qbeastSnapshot.dimensionCount),
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
          files.filter(_.tags(cubeTag) == cubeString)
        case Some(cubeWeight) =>
          val cubeString = cube.string
          val childFiles = cube.children
            .filter(space.intersectsWith)
            .flatMap(doFindSampleFiles)
          if (!replicatedSet.contains(cube) && precision.from < cubeWeight) {
            val cubeFiles = files.filter(_.tags(cubeTag) == cubeString)
            if (childFiles.nonEmpty) {
              cubeFiles.filterNot(_.tags(stateTag) == ANNOUNCED) ++ childFiles
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
