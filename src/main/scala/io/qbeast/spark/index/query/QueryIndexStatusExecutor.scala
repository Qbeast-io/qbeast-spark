/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.utils.State
import scala.collection.immutable.SortedMap

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
      previouslyMatchedFiles: Seq[QbeastFile]): IISeq[QbeastFile] = {

    implicit val cubeIdOrdering: Ordering[CubeId] = Ordering.by(_.string)
    val sortedCubeStatuses = SortedMap[CubeId, CubeStatus]() ++ cubesStatuses

    val fileMap = previouslyMatchedFiles.map(a => (a.path, a)).toMap

    def doFindSampleFilesWithSortedMap(cube: CubeId): IISeq[QbeastFile] = {
      val cubeIter = sortedCubeStatuses.iteratorFrom(cube)
      if (cubeIter.hasNext) {
        val currentCube = cubeIter.next()
        currentCube match {
          case (c: CubeId, CubeStatus(maxWeight, _, files)) if c == cube =>
            if (maxWeight > weightRange.to) {
              files.flatMap(fileMap.get)
            } else {
              val childFiles = c.children
                .filter(space.intersectsWith)
                .flatMap(doFindSampleFilesWithSortedMap)
              if (replicatedSet.contains(cube) || weightRange.from >= maxWeight) {
                childFiles.toVector
              } else {
                val cubeFiles = files.flatMap(fileMap.get)
                if (childFiles.nonEmpty) {
                  cubeFiles.filterNot(_.state == State.ANNOUNCED) ++ childFiles
                } else {
                  cubeFiles
                }
              }
            }
          case (c: CubeId, CubeStatus(maxWeight, _, files)) if c.string.startsWith(cube.string) =>
            cube.children
              .filter(space.intersectsWith)
              .flatMap(doFindSampleFilesWithSortedMap)
              .toVector
          case _ => Vector.empty
        }
      } else {
        Vector.empty
      }
    }
    doFindSampleFilesWithSortedMap(startCube)
  }

}
