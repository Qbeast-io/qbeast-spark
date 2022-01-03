/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model.{CubeId, CubeStatus, IndexStatus, QbeastFile, QbeastSnapshot, QuerySpace, WeightRange}
import io.qbeast.spark.utils.State

import scala.collection.immutable.SortedMap
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
      val indexStatusExecutor = new QueryIndexStatusExecutor(querySpec, indexStatus)

      val matchingFiles = indexStatusExecutor.execute(previouslyMatchedFiles)
      matchingFiles
    }
  }

  /**
   * Executes a query against a index status
   * @param querySpec the query specification
   * @param indexStatus the index status
   */
  class QueryIndexStatusExecutor(querySpec: QuerySpec, indexStatus: IndexStatus) {

    def execute(previouslyMatchedFiles: Seq[QbeastFile]): IISeq[QbeastFile] = {

      findSampleFiles(
        querySpec.querySpace,
        querySpec.weightRange,
        indexStatus.revision.createCubeIdRoot(),
        indexStatus.cubesStatuses,
        indexStatus.replicatedSet,
        indexStatus.announcedSet,
        previouslyMatchedFiles)
    }

    /**
     * Finds the files to retrieve the query sample.
     *
     * @param space the query space
     * @param weightRange the weight range
     * @param startCube the start cube
     * @param cubesStatuses the cube weights and files
     * @param replicatedSet the replicated set
     * @param announcedSet the announced set
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
                         announcedSet: Set[CubeId],
                         previouslyMatchedFiles: Seq[QbeastFile]): IISeq[QbeastFile] = {

      implicit val cubeIdOrdering: Ordering[CubeId] = Ordering.by(_.string)
      val sortedCubeStatuses = SortedMap[CubeId, CubeStatus]() ++ cubesStatuses

      val fileMap = previouslyMatchedFiles.map(a => (a.path, a)).toMap

      val outputFiles = Vector.newBuilder[QbeastFile]
      outputFiles.sizeHint(sortedCubeStatuses.size)
      val firstLevel = Vector(startCube)
      val queue = Queue(firstLevel)
      while (queue.nonEmpty) {
        val currentLevel = queue.dequeue()
        currentLevel.foreach(currentCube => {
          val cubeIter = sortedCubeStatuses.iteratorFrom(currentCube)
          // Contains cases for the next element from the iterator being
          // 1. the cube itself
          // 2. one of the cube's children
          // 3. this currentCube's sibling or their subtree
          // 4. empty, the currentCube is the right-most cube in the tree
          // and is not in cubesStatuses
          if (cubeIter.hasNext) { // cases 1 to 3
            cubeIter.next() match {
              case (c: CubeId, CubeStatus(maxWeight, _, files))
                if c == currentCube && weightRange.from < maxWeight => // Case 1
                if (weightRange.to < maxWeight) {
                  // cube maxWeight is larger than the sample fraction, weightRange.to,
                  // it means that currentCube is the last cube to visit from the current branch.
                  // All files are retrieved and no more cubes from the branch will be visited.
                  outputFiles ++= files.flatMap(fileMap.get)
                } else {
                  // Otherwise,
                  // 1. if the currentCube is REPLICATED, we skip the cube
                  // 2. if the state is ANNOUNCED, ignore the After Announcement elements
                  // 3. if FLOODED, retrieve all files from the cube
                  val (isReplicated, isAnnounced, hasChildren) =
                  (replicatedSet.contains(c), announcedSet.contains(c), c.children.nonEmpty)
                  val cubeFiles =
                    if (isReplicated) {
                      Vector.empty
                    } else if (isAnnounced && hasChildren) {
                      files.flatMap(fileMap.get).filterNot(_.state == State.ANNOUNCED)
                    } else {
                      files.flatMap(fileMap.get)
                    }
                  outputFiles ++= cubeFiles
                  queue.enqueue(
                    c.children
                      .filter(space.intersectsWith)
                      .toVector)
                }

              case (c: CubeId, CubeStatus(maxWeight, _, _))
                if c.string.startsWith(
                  currentCube.string) && weightRange.from < maxWeight => // Case 2
                // c is a child cube of currentCube. Aside from c, we also need to
                // consider c's sibling cubes.
                queue.enqueue(
                  currentCube.children
                    .filter(space.intersectsWith)
                    .toVector)

              case _ => // Case 3
            }
          }
        })
      }
      outputFiles.result()
    }

  }

}
