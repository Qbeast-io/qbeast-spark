/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.utils.State

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

    val keys = cubesStatuses.keys.map(_.string).toVector
    def haveImissingDescendands(cube: CubeId): Boolean = {
      val c = cube.string
      keys.count(cubeString => cubeString.startsWith(c)) > 1
    }
    val fileMap = previouslyMatchedFiles.map(a => (a.path, a)).toMap
    def doFindSampleFiles(cube: CubeId): IISeq[QbeastFile] = {
      cubesStatuses.get(cube) match {
        case Some(CubeStatus(maxWeight, _, files)) if weightRange.to < maxWeight =>
          files.flatMap(fileMap.get)
        case Some(CubeStatus(maxWeight, _, files)) =>
          val childFiles: Iterator[QbeastFile] = cube.children
            .filter(space.intersectsWith)
            .flatMap(doFindSampleFiles)
          if (!replicatedSet.contains(cube) && weightRange.from < maxWeight) {
            val cubeFiles = files.flatMap(fileMap.get)
            if (childFiles.nonEmpty) {
              cubeFiles.filterNot(_.state == State.ANNOUNCED) ++ childFiles
            } else {
              cubeFiles
            }
          } else {
            childFiles.toVector
          }
        case None =>
          // TODO how we manage non-existing/empty cubes if the children are present?
          if (haveImissingDescendands(cube)) {
            cube.children
              .filter(space.intersectsWith)
              .flatMap(doFindSampleFiles)
              .toVector
          } else {
            Vector.empty
          }
      }
    }

    doFindSampleFiles(startCube)
  }

}
