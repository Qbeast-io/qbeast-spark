/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.expressions.QbeastSample
import io.qbeast.core.model.{
  CubeStatus,
  IndexStatus,
  QuerySpace,
  QbeastBlock,
  Revision,
  StagingUtils,
  Weight,
  WeightRange
}
import io.qbeast.spark.utils.State
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.AddFile
import scala.collection.mutable.{Builder, HashMap, Stack}

/**
 * Query executes queries againts a specified Qbeast table snapshot.
 *
 * @param snapshot the snapshot to use for executing queries
 */
class QueryEngine(snapshot: DeltaQbeastSnapshot) extends StagingUtils {

  /**
   * Executes the query defined in terms of given filters.
   *
   * @param filters the query filters
   * @returns the query result divided into parts, where each part represents
   * the data to be read from a single table file
   */
  def executeQuery(filters: Seq[Expression]): Seq[ResultPart] = {
    val parts = Seq.newBuilder[ResultPart]
    val spacesFactory = new QuerySpacesFactory(filters)
    val range = getWeightRange(filters)
    if (!range.isEmpty) {
      snapshot.loadAllRevisions.foreach(queryRevision(parts, spacesFactory, range))
    }
    parts.result()
  }

  private def getWeightRange(filters: Seq[Expression]): WeightRange = {
    filters
      .find(_.isInstanceOf[QbeastSample])
      .map(_.asInstanceOf[QbeastSample])
      .map(_.toWeightRange())
      .getOrElse(WeightRange.All)
  }

  private def queryRevision(
      parts: Builder[ResultPart, _],
      spacesFactory: QuerySpacesFactory,
      range: WeightRange)(revision: Revision): Unit = {
    if (isStaging(revision)) {
      queryStagingArea(parts, range)
    } else {
      val index = snapshot.loadIndexStatus(revision.revisionID)
      val spaces = spacesFactory.newQuerySpaces(revision)
      queryIndex(parts, spaces, range, index)
    }
  }

  private def queryStagingArea(parts: Builder[ResultPart, _], range: WeightRange): Unit = {
    snapshot.loadStagingBlocks().foreach(queryStagingFile(parts, range)(_))
  }

  private def queryStagingFile(parts: Builder[ResultPart, _], range: WeightRange)(
      file: AddFile): Unit = {
    val scale =
      file.getNumLogicalRecords.toDouble / (Weight.MaxValue.value - Weight.MinValue.value)
    val WeightRange(Weight(weightFrom), Weight(weightTo)) = range
    val from = ((weightFrom - Weight.MinValue.value) * scale).toLong
    val to = ((weightTo - Weight.MinValue.value) * scale).toLong
    parts += ResultPart(file.path, file.size, file.modificationTime, from, to)
  }

  private def queryIndex(
      parts: Builder[ResultPart, _],
      spaces: Seq[Seq[QuerySpace]],
      range: WeightRange,
      index: IndexStatus): Unit = {
    val head :: tail = spaces
    var partMap = querySpacesUnion(head, range, index, _ => true)
    for (union <- tail) {
      partMap = querySpacesUnion(union, range, index, partMap.contains)
    }
    parts ++= partMap.values
  }

  private def querySpacesUnion(
      union: Seq[QuerySpace],
      range: WeightRange,
      index: IndexStatus,
      pathFilter: String => Boolean): HashMap[String, ResultPart] = {
    val parts = HashMap.empty[String, ResultPart]
    union.foreach(querySpace(parts, range, index, pathFilter))
    parts
  }

  private def querySpace(
      parts: HashMap[String, ResultPart],
      range: WeightRange,
      index: IndexStatus,
      pathFilter: String => Boolean)(space: QuerySpace): Unit = {
    val stack = Stack(index.revision.createCubeIdRoot())
    while (stack.nonEmpty) {
      val cubeId = stack.pop()
      // Child cubes have identifiers greater than the parent's identifier
      val statusIterator = index.cubesStatuses.iteratorFrom(cubeId)
      if (statusIterator.hasNext) {
        statusIterator.next() match {
          // Process the status of the current cubeId
          case (aCubeId, CubeStatus(_, maxWeight, _, blocks)) if (aCubeId == cubeId) => {
            // By default include all the blocks
            var blockIterator = blocks.iterator
            // Cube does not have enough data
            if (range.to >= maxWeight) {
              if (index.replicatedSet.contains(cubeId)) {
                // Skip replicated cube
                blockIterator = Iterator.empty
              } else if (index.announcedSet.contains(cubeId)) {
                // Include only those blocks which have not been announced
                blockIterator = blockIterator.filter(_.state != State.ANNOUNCED)
              }
              // Push child cubes into stack o get more data
              cubeId.children.filter(space.intersectsWith).foreach(stack.push)
            }
            blockIterator
              // Include only blocks with elements from the weight range
              .filter(_.maxWeight > range.from)
              // Include only blocks with path matching the filter
              .filter(block => pathFilter(block.path))
              .map(qbeastBlockToResultPart(range))
              .foreach(part => parts.put(part.filePath, part))
          }
          // Sometimes cube identifier does not have the corresponding status
          // It happens when the estimated weight of the child cube is equal to
          // the estimated weight of its parent. In such a case the data which
          // does not fit parent is saved in the grand child cubes instead of
          // the child cube. To continue the query the children og the current
          // cube are pushed into the stack
          case (aCubeId, _) if (cubeId.isAncestorOf(aCubeId)) => {
            cubeId.children.filter(space.intersectsWith).foreach(stack.push)
          }
        }
      }
    }
  }

  private def qbeastBlockToResultPart(range: WeightRange)(block: QbeastBlock): ResultPart = {
    val scale = block.elementCount.toDouble / (block.maxWeight.value - block.minWeight.value)
    val minValue = math.max(block.minWeight.value, range.from.value)
    val maxValue = math.min(block.maxWeight.value, range.to.value - 1)
    val from = ((minValue - block.minWeight.value) * scale).floor.toLong
    val to = ((maxValue - block.minWeight.value) * scale).ceil.toLong
    ResultPart(block.path, block.size, block.modificationTime, from, to)
  }

}
