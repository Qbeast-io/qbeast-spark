/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.files

import io.qbeast.spark.index.{CubeId, Weight}
import io.qbeast.spark.model.QuerySpace
import io.qbeast.spark.sql.qbeast.RevisionData
import io.qbeast.spark.sql.utils.QbeastExpressionUtils.extractQuerySpace
import io.qbeast.spark.sql.utils.{State, TagUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.AddFile

/**
 * Trait that allows filtering the files for a certain revision and query conditions
 */
trait RevisionFiltering { revisionData: RevisionData =>

  /**
   * Given weight from-to range and a sequence of filters, initialize QuerySpace
   * and find the files that satisfy the predicates
   *
   * @param fromWeight from weight
   * @param toWeight to weight
   * @param dataFilters the filters issued by the spark plan
   * @return the sequence of files of the specific revision matching the query predicates
   */
  def sample(fromWeight: Weight, toWeight: Weight, dataFilters: Seq[Expression]): Seq[AddFile] = {

    if (toWeight == Weight.MinValue || fromWeight > toWeight) return Seq.empty

    val revision = revisionData.revision
    val cubeWeights = revisionData.cubeWeights
    val replicatedSet = revisionData.replicatedSet
    // TODO should we implement this with Dataset interface?
    val filesRevision = revisionData.files.collect().toVector

    val querySpace = extractQuerySpace(dataFilters, revision, SparkSession.active)

    findSampleFiles(
      querySpace,
      fromWeight,
      toWeight,
      CubeId.root(revision.dimensionCount),
      filesRevision,
      cubeWeights,
      replicatedSet)
  }

  /**
   * Finds the files to retrieve the query sample.
   *
   * @param space the query space
   * @param fromWeight the sample lower bound
   * @param toWeight the sample upper bound
   * @param startCube the start cube
   * @param files the data files
   * @param cubeWeights the cube weights
   * @return the files with sample data
   */
  def findSampleFiles(
      space: QuerySpace,
      fromWeight: Weight,
      toWeight: Weight,
      startCube: CubeId,
      files: Vector[AddFile],
      cubeWeights: Map[CubeId, Weight],
      replicatedSet: Set[CubeId]): Vector[AddFile] = {

    def doFindSampleFiles(cube: CubeId): Vector[AddFile] = {
      cubeWeights.get(cube) match {
        case Some(cubeWeight) if toWeight < cubeWeight =>
          val cubeString = cube.string
          files.filter(_.tags(TagUtils.cube) == cubeString)
        case Some(cubeWeight) =>
          val cubeString = cube.string
          val childFiles = cube.children
            .filter(space.intersectsWith)
            .map(doFindSampleFiles)
            .reduce(_ union _)
          if (!replicatedSet.contains(cube) && fromWeight < cubeWeight) {
            val cubeFiles = files.filter(_.tags(TagUtils.cube) == cubeString)
            if (childFiles.nonEmpty) {
              cubeFiles.filter(_.tags(TagUtils.state) != State.ANNOUNCED).union(childFiles)
            } else {
              cubeFiles
            }
          } else {
            childFiles
          }
        case None => Vector.empty
      }
    }

    doFindSampleFiles(startCube)
  }

}
