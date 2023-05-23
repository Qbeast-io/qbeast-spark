/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model.{
  AllSpace,
  CubeStatus,
  EmptySpace,
  IndexStatus,
  QbeastBlock,
  QuerySpace,
  QuerySpaceFromTo,
  Revision,
  StagingUtils,
  Weight,
  WeightRange
}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.expressions.QbeastSample
import io.qbeast.spark.utils.State
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.AddFile
import scala.collection.mutable
import org.apache.spark.sql.SparkSession

/**
 * Query to retrive data from the Qbeast table. Query is a one-time object
 * it should not be reused.
 *
 * @param spark the Spark session
 * @param snapshot the table snapshot to retrive the data from
 * @param filters the filters to apply while retrieving the data
 */
class Query(spark: SparkSession, snapshot: DeltaQbeastSnapshot, filters: Seq[Expression])
    extends StagingUtils {
  private val spacesFactory = new QuerySpacesFactory(spark, filters)

  private val range = filters.collectFirst { case s: QbeastSample =>
    s.toWeightRange()
  }

  private val parts = Seq.newBuilder[ResultPart]

  /**
   * Executes the query.
   *
   * @return the query result divided into parts, where each part represents the
   * data to read from a single table file
   */
  def execute(): Seq[ResultPart] = {
    parts.clear()
    if (range.isDefined && range.get.isEmpty) {
      return Seq.empty
    }
    snapshot.loadAllRevisions.foreach(queryRevision)
    parts.result()
  }

  private def queryRevision(revision: Revision): Unit = {
    if (isStaging(revision)) {
      queryStagingArea()
    } else {
      queryIndexRevision(revision)
    }
  }

  private def queryStagingArea(): Unit = {
    snapshot.loadStagingBlocks().foreach(queryStagingBlock(_))
  }

  private def queryStagingBlock(block: AddFile): Unit = {
    val count = block.getNumLogicalRecords
    val (from, to) = range match {
      case Some(WeightRange(weightFrom, weightTo)) =>
        ((weightFrom.fraction * count).floor.toLong, (weightTo.fraction * count).ceil.toLong)
      case None => (0L, count)
    }
    parts += ResultPart(block.path, block.size, block.modificationTime, from, to)
  }

  private def queryIndexRevision(revision: Revision): Unit = {
    val head :: tail = spacesFactory.newQuerySpaces(revision)
    val index = snapshot.loadIndexStatus(revision.revisionID)
    var intersectionParts = querySpaceUnion(head, index, _ => true)
    for (union <- tail) {
      intersectionParts = querySpaceUnion(union, index, intersectionParts.contains)
    }
    parts ++= intersectionParts.values
  }

  private def querySpaceUnion(
      union: Seq[QuerySpace],
      index: IndexStatus,
      pathFilter: String => Boolean): mutable.Map[String, ResultPart] = {
    val unionParts = mutable.HashMap.empty[String, ResultPart]
    union.foreach(space => querySpace(space, index, pathFilter, unionParts))
    unionParts
  }

  private def querySpace(
      space: QuerySpace,
      index: IndexStatus,
      pathFilter: String => Boolean,
      unionParts: mutable.Map[String, ResultPart]): Unit = {
    space match {
      case _: QuerySpaceFromTo => queryOTree(space, index, pathFilter, unionParts)
      case _: AllSpace if range.isDefined => queryOTree(space, index, pathFilter, unionParts)
      case _: AllSpace => queryAllFiles(index, pathFilter, unionParts)
      case _: EmptySpace => ()
    }
  }

  private def queryOTree(
      space: QuerySpace,
      index: IndexStatus,
      pathFilter: String => Boolean,
      unionParts: mutable.Map[String, ResultPart]): Unit = {
    val statuses = index.cubesStatuses
    val stack = mutable.Stack(index.revision.createCubeIdRoot())
    while (stack.nonEmpty) {
      val cubeId = stack.pop()
      // Child cubes have id greater than the parent cube id, so their status
      // will appear after the parent cube status
      val statusIterator = statuses.iteratorFrom(cubeId)
      if (statusIterator.hasNext) {
        statusIterator.next() match {
          // Query the current cube
          case (aCubeId, CubeStatus(_, maxWeight, _, files)) if aCubeId == cubeId =>
            // By default include all cube files
            var fileIterator = files.iterator
            if (range.isDefined) {
              if (range.get.to > maxWeight) {
                // Cube does not have enough data
                if (index.replicatedSet.contains(cubeId)) {
                  // Replicated cube should be skipped
                  fileIterator = Iterator.empty
                } else if (index.announcedSet.contains(cubeId)) {
                  // Filter out the files which are announced for replication
                  fileIterator = fileIterator.filter(_.state != State.ANNOUNCED)
                }
              }
              // Files should have data with weight within the specifed range
              fileIterator = fileIterator.filter(_.maxWeight > range.get.from)
            }
            fileIterator
              .filter(file => pathFilter(file.path))
              .map(file => queryFile(file, range))
              .foreach(part => unionParts.put(part.filePath, part))
          // Due to how the cube weight is estimated, sometimes it happens
          // that the child cubes are missing in the index and the data is
          // stored in the grand child cubes. In such a case push the child
          // cube identifiers into the stack to continue the query
          case (aCubeId, _) if cubeId.isAncestorOf(aCubeId) =>
            cubeId.children.filter(space.intersectsWith(_)).foreach(stack.push)
          case _ => ()
        }
      }
    }

  }

  private def queryFile(file: QbeastBlock, range: Option[WeightRange]): ResultPart = {
    val count = file.elementCount
    val fileMinValue = file.minWeight.value
    val fileMaxValue = file.maxWeight.value
    val (from, to) =
      if (range.isEmpty || fileMinValue == fileMaxValue) {
        (0L, count)
      } else {
        val WeightRange(Weight(rangeFromValue), Weight(rangeToValue)) = range.get
        val minValue = math.max(fileMinValue, rangeFromValue)
        val maxValue = math.min(fileMaxValue, rangeToValue - 1)
        val scale = count.toDouble / (fileMaxValue - fileMinValue)
        (
          ((minValue - fileMinValue) * scale).floor.toLong,
          ((maxValue - fileMinValue) * scale).ceil.toLong)
      }
    ResultPart(file.path, file.size, file.modificationTime, from, to)
  }

  private def queryAllFiles(
      index: IndexStatus,
      pathFilter: String => Boolean,
      unionParts: mutable.Map[String, ResultPart]): Unit = {
    index.cubesStatuses.values
      .flatMap(_.files)
      .filter(_.state == State.FLOODED)
      .filter(file => pathFilter(file.path))
      .map(file => queryFile(file, None))
      .foreach(part => unionParts.put(part.filePath, part))
  }

}
