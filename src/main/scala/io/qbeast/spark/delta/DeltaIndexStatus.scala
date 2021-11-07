/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{CubeId, IndexStatus, NormalizedWeight, Revision, Weight}
import io.qbeast.spark.index.ReplicatedSet
import io.qbeast.spark.utils.{State, TagUtils}
import io.qbeast.spark.index.writer.BlockStats
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions.{min, sum}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Cube Information
 * @param cube Id of the cube
 * @param maxWeight Maximum weight of the cube
 * @param size Number of elements of the cube
 */

case class CubeInfo(cube: String, maxWeight: Weight, size: Long)

/**
 * Snapshot that provides information about the current index state of a given Revision.
 * @param revision the given Revision
 * @param replicatedSet the set of replicated cubes for the specific Revision
 * @param files dataset of AddFiles that belongs to the specific Revision
 */
case class DeltaIndexStatus(
    revision: Revision,
    replicatedSet: ReplicatedSet,
    files: Dataset[AddFile])
    extends IndexStatus {

  /**
   * Returns the cube maximum weights for a given space revision
   * @return a map with key cube and value max weight
   */
  def cubeWeights: Map[CubeId, Weight] = {
    revisionState
      .collect()
      .map(info => (revision.createCubeId(info.cube), info.maxWeight))
      .toMap
  }

  /**
   * Returns the cube maximum normalized weights for a given space revision
   *
   * @return a map with key cube and value max weight
   */
  def cubeNormalizedWeights: Map[CubeId, NormalizedWeight] = {
    revisionState
      .collect()
      .map {
        case CubeInfo(cube, Weight.MaxValue, size) =>
          (revision.createCubeId(cube), NormalizedWeight(revision.desiredCubeSize, size))
        case CubeInfo(cube, maxWeight, _) =>
          (revision.createCubeId(cube), NormalizedWeight(maxWeight))
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
      .map(cubeInfo => revision.createCubeId(cubeInfo.cube))
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
      .filter(a => cubes.contains(revision.createCubeId(a.tags(TagUtils.cube))))
      .collect()
  }

  override def announcedSet: Set[CubeId] = Set.empty
}

/**
 * Companion object for RevisionData
 */
object DeltaIndexStatus {

  /**
   * Build a new RevisionData for a revision with empty replicatedSet and empty files
   *
   * @param revision the revision
   * @return revision data
   */
  def apply(revision: Revision): DeltaIndexStatus = {
    val spark = SparkSession.active
    import spark.implicits._

    DeltaIndexStatus(revision, Set.empty, spark.createDataset(Seq.empty[AddFile]))
  }

}
