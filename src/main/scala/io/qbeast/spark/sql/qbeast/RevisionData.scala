/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.{CubeId, NormalizedWeight, ReplicatedSet, Weight}
import io.qbeast.spark.model.{CubeInfo, Revision}
import io.qbeast.spark.sql.utils.{State, TagUtils}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions.{min, sum}

/**
 * Snapshot that provides information about the current index state of a given Revision.
 * @param revision the given Revision
 * @param replicatedSet the set of replicated cubes for the specific Revision
 * @param files dataset of AddFiles that belongs to the specific Revision
 */
case class RevisionData(
    revision: Revision,
    replicatedSet: ReplicatedSet,
    files: Dataset[AddFile]) {

  private def dimensionCount = revision.dimensionCount

  /**
   * Returns the cube maximum weights for a given space revision
   * @return a map with key cube and value max weight
   */
  def cubeWeights: Map[CubeId, Weight] = {
    revisionState
      .collect()
      .map(info => (CubeId(dimensionCount, info.cube), info.maxWeight))
      .toMap
  }

  /**
   * Returns the cube maximum normalized weights for a given space revision
   *
   * @return a map with key cube and value max weight
   */
  def cubeNormalizedWeights: Map[CubeId, Double] = {
    revisionState
      .collect()
      .map {
        case CubeInfo(cube, Weight.MaxValue, size) =>
          (
            CubeId(revision.dimensionCount, cube),
            NormalizedWeight(revision.desiredCubeSize, size))
        case CubeInfo(cube, maxWeight, _) =>
          (CubeId(dimensionCount, cube), NormalizedWeight(maxWeight))
      }
      .toMap
  }

  /**
   * Returns the set of cubes that are overflowed for a given space revision
   * @return the set of overflowed cubes
   */

  def overflowedSet: Set[CubeId] = {
    revisionState
      .filter(_.maxWeight != Weight.MaxValue)
      .collect()
      .map(cubeInfo => CubeId(dimensionCount, cubeInfo.cube))
      .toSet
  }

  /**
   * Returns the index state for the given space revision
   * @return Dataset containing cube information
   */
  private def revisionState: Dataset[CubeInfo] = {

    val spark = SparkSession.active
    import spark.implicits._

    val weightValueTag = TagUtils.maxWeight + ".value"

    files
      .map(a =>
        BlockStats(
          a.tags(TagUtils.cube),
          Weight(a.tags(TagUtils.maxWeight).toInt),
          Weight(a.tags(TagUtils.minWeight).toInt),
          a.tags(TagUtils.state),
          a.tags(TagUtils.elementCount).toLong))
      .groupBy(TagUtils.cube)
      .agg(min(weightValueTag), sum(TagUtils.elementCount))
      .map(row => CubeInfo(row.getAs[String](0), Weight(row.getAs[Int](1)), row.getAs[Long](2)))
  }

  /**
   * Returns the sequence of blocks for a set of cubes belonging to a specific space revision
   * @param cubes the set of cubes
   * @return the sequence of blocks
   */
  def getCubeBlocks(cubes: Set[CubeId]): Seq[AddFile] = {
    files
      .filter(_.tags(TagUtils.state) != State.ANNOUNCED)
      .filter(a => cubes.contains(CubeId(dimensionCount, a.tags(TagUtils.cube))))
      .collect()
  }

}
