/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.IISeq

import scala.collection.mutable

/**
 * Executes a query against a Qbeast snapshot
 * @param querySpecBuilder
 *   the builder for the query specification
 */
class QueryExecutor(querySpecBuilder: QuerySpecBuilder, qbeastSnapshot: QbeastSnapshot) {

  /**
   * Executes the query on each revision according to their QuerySpec
   * @return
   *   the final sequence of blocks that match the query
   */
  def execute(): Iterable[Block] = {

    qbeastSnapshot.loadAllRevisions.flatMap { revision =>
      val querySpecs = querySpecBuilder.build(revision)
      querySpecs.flatMap { querySpec =>
        (querySpec.isSampling, querySpec.querySpace) match {
          case (_, _: QuerySpaceFromTo) | (true, _: AllSpace) =>
            val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
            val matchingBlocks = executeRevision(querySpec, indexStatus)
            matchingBlocks
          case (false, _: AllSpace) =>
            val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
            indexStatus.cubesStatuses.values.flatMap { status =>
              status.blocks.filterNot(_.replicated)
            }
          case _ => Seq.empty[Block]
        }
      }
    }.toSet
  }

  private[query] def executeRevision(
      querySpec: QuerySpec,
      indexStatus: IndexStatus): IISeq[Block] = {

    val outputBlocks = Vector.newBuilder[Block]
    val stack = mutable.Stack(indexStatus.revision.createCubeIdRoot())
    while (stack.nonEmpty) {
      val currentCube = stack.pop()

      val cubeIter = indexStatus.cubesStatuses.iteratorFrom(currentCube)
      // Contains cases for the next element from the iterator being
      // 1. the cube itself
      // 2. one of the cube's children
      // 3. this currentCube's sibling or their subtree
      // 4. empty, the currentCube is the right-most cube in the tree and it is not in cubesStatuses
      if (cubeIter.hasNext) { // cases 1 to 3
        cubeIter.next() match {
          case (cube, CubeStatus(_, maxWeight, _, blocks)) if cube == currentCube => // Case 1
            val unfilteredBlocks = if (querySpec.weightRange.to <= maxWeight) {
              // cube maxWeight is larger than or equal to the sample fraction (weightRange.to),
              // that currentCube is the last cube to visit from the current branch - all blocks
              // are to be retrieved and no more cubes from the branch should be visited.
              blocks
            } else {
              // Otherwise,
              // 1. if the currentCube is REPLICATED, we skip the cube
              // 2. if the state is ANNOUNCED, ignore the After Announcement elements
              // 3. if FLOODED, retrieve all files from the cube
              val isReplicated = indexStatus.replicatedSet.contains(cube)
              val isAnnounced = indexStatus.announcedSet.contains(cube)
              val cubeFiles =
                if (isReplicated) {
                  Vector.empty
                } else if (isAnnounced) {
                  blocks.filterNot(_.replicated)
                } else {
                  blocks
                }
              val nextLevel = cube.children
                .filter(querySpec.querySpace.intersectsWith)
              stack.pushAll(nextLevel)
              cubeFiles
            }

            // Blocks should have overlapping weight ranges with that from the query.
            // Note that blocks don't contain records with weight = block.minWeight, which are
            // contained in their direct parent cubes.
            outputBlocks ++= unfilteredBlocks
              .filter(block =>
                querySpec.weightRange.from <= block.maxWeight
                  && querySpec.weightRange.to > block.minWeight)

          case (cube, _) if currentCube.isAncestorOf(cube) => // Case 2
            // cube is a descendant of currentCube, and currentCube is missing.
            // We proceed navigating the subtree.
            val nextLevel = currentCube.children
              .filter(querySpec.querySpace.intersectsWith)
            stack.pushAll(nextLevel)

          case _ => // Case 3
        }
      }
    }
    outputBlocks.result()
  }

}
