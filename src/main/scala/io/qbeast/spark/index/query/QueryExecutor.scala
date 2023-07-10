/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model._

import scala.collection.mutable

/**
 * Executes a query against a Qbeast snapshot
 * @param querySpecBuilder the builder for the query specification
 */
class QueryExecutor(querySpecBuilder: QuerySpecBuilder, qbeastSnapshot: QbeastSnapshot) {

  /**
   * Executes the query on each revision according to their QuerySpec
   * @return the final sequence of blocks that match the query
   */
  def execute(): Iterable[QbeastBlock] = {

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
              status.files.filterNot(_.replicated)
            }
          case _ => Seq.empty[QbeastBlock]
        }
      }
    }.toSet
  }

  private[query] def executeRevision(
      querySpec: QuerySpec,
      indexStatus: IndexStatus): IISeq[QbeastBlock] = {

    val outputFiles = Vector.newBuilder[QbeastBlock]
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
          case (cube, CubeStatus(_, maxWeight, _, files)) if cube == currentCube => // Case 1
            val unfilteredFiles = if (querySpec.weightRange.to < maxWeight) {
              // cube maxWeight is larger than the sample fraction, weightRange.to,
              // it means that currentCube is the last cube to visit from the current branch.
              // All files are retrieved and no more cubes from the branch will be visited.
              files
            } else {
              // cube does not have enough data, read non replicated files and
              // proceed to the child cubes
              val nextLevel = cube.children
                .filter(querySpec.querySpace.intersectsWith)
              stack.pushAll(nextLevel)
              files.filterNot(_.replicated)
            }

            outputFiles ++= unfilteredFiles.filter(file =>
              file.maxWeight > querySpec.weightRange.from)

          case (cube, _) if currentCube.isAncestorOf(cube) => // Case 2
            // c is a child cube of currentCube. Aside from c, we also need to
            // consider c's sibling cubes.
            val nextLevel = currentCube.children
              .filter(querySpec.querySpace.intersectsWith)
            stack.pushAll(nextLevel)

          case _ => // Case 3
        }
      }
    }
    outputFiles.result()
  }

}
