/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.utils.State

import scala.collection.mutable.Queue

/**
 * Executes a query against a Qbeast snapshot
 * @param querySpecBuilder the builder for the query specification
 */
class QueryExecutor(querySpecBuilder: QuerySpecBuilder, qbeastSnapshot: QbeastSnapshot) {

  /**
   * Executes the query given a previous matched files
   * @param previouslyMatchedFiles the sequence of files that have already been matched
   * @return the final sequence of files that match the query
   */
  def execute(previouslyMatchedFiles: Seq[QbeastFile]): Seq[QbeastFile] = {

    qbeastSnapshot.loadAllRevisions.flatMap { revision =>
      val querySpec = querySpecBuilder.build(revision)
      val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)

      val matchingFiles = executeRevision(querySpec, indexStatus, previouslyMatchedFiles)
      matchingFiles
    }
  }

  private[query] def executeRevision(
      querySpec: QuerySpec,
      indexStatus: IndexStatus,
      previouslyMatchedFiles: Seq[QbeastFile]): IISeq[QbeastFile] = {

    val fileMap = previouslyMatchedFiles.map(a => (a.path, a)).toMap

    val outputFiles = Vector.newBuilder[QbeastFile]
    val queue = Queue(indexStatus.revision.createCubeIdRoot())
    while (queue.nonEmpty) {
      val currentCube = queue.dequeue()

      val cubeIter = indexStatus.cubesStatuses.iteratorFrom(currentCube)
      // Contains cases for the next element from the iterator being
      // 1. the cube itself
      // 2. one of the cube's children
      // 3. this currentCube's sibling or their subtree
      // 4. empty, the currentCube is the right-most cube in the tree and it is not in cubesStatuses
      if (cubeIter.hasNext) { // cases 1 to 3
        cubeIter.next() match {
          case (cube, CubeStatus(maxWeight, _, files)) if cube == currentCube => // Case 1
            if (querySpec.weightRange.to < maxWeight) {
              // cube maxWeight is larger than the sample fraction, weightRange.to,
              // it means that currentCube is the last cube to visit from the current branch.
              // All files are retrieved and no more cubes from the branch will be visited.
              outputFiles ++= files.flatMap(fileMap.get)
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
                  files.flatMap(fileMap.get).filterNot(_.state == State.ANNOUNCED)
                } else {
                  files.flatMap(fileMap.get)
                }
              outputFiles ++= cubeFiles.filter(file =>
                file.maxWeight > querySpec.weightRange.from)

              cube.children
                .filter(querySpec.querySpace.intersectsWith)
                .foreach(queue.enqueue(_))
            }

          case (cube, _) if currentCube.isAncestorOf(cube) => // Case 2
            // c is a child cube of currentCube. Aside from c, we also need to
            // consider c's sibling cubes.
            currentCube.children
              .filter(querySpec.querySpace.intersectsWith)
              .foreach(queue.enqueue(_))

          case _ => // Case 3
        }
      }
    }
    outputFiles.result()
  }

}
