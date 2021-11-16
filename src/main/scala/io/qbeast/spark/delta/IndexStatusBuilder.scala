/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{CubeId, IndexStatus, NormalizedWeight, ReplicatedSet, Revision, Weight}
import io.qbeast.spark.index.writer.BlockStats
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions.{min, sum}

/**
 * Cube Information
 *
 * @param cube      Id of the cube
 * @param maxWeight Maximum weight of the cube
 * @param size      Number of elements of the cube
 */

case class CubeInfo(cube: String, maxWeight: Weight, size: Long)

private[delta] class IndexStatusBuilder(
    qbeastSnapshot: DeltaQbeastSnapshot,
    revision: Revision,
    announcedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  def revisionFiles: Dataset[AddFile] = {
    // this must be external to the lambda, to avoid SerializationErrors
    val revID = revision.revisionID.toString
    qbeastSnapshot.snapshot.allFiles.filter(_.tags(TagUtils.revision) == revID)
  }

  def replicatedSet: ReplicatedSet =
    qbeastSnapshot.getReplicatedSet(revision.revisionID)

  def build(): IndexStatus = {
    IndexStatus(
      revision = revision,
      replicatedSet = replicatedSet,
      announcedSet = announcedSet,
      cubeWeights = cubeWeights,
      cubeNormalizedWeights = cubeNormalizedWeights,
      overflowedSet = overflowedSet)
  }

  /**
   * Returns the cube maximum weights for a given space revision
   *
   * @return a map with key cube and value max weight
   */
  def cubeWeights: Map[CubeId, Weight] = {
    revisionState
      .map(info => (revision.createCubeId(info.cube), info.maxWeight))
      .toMap
  }

  /**
   * Returns the cube maximum normalized weights for a given space revision
   *
   * @return a map with key cube and value max weight
   */
  def cubeNormalizedWeights: Map[CubeId, NormalizedWeight] = {
    revisionState.map {
      case CubeInfo(cube, Weight.MaxValue, size) =>
        (revision.createCubeId(cube), NormalizedWeight(revision.desiredCubeSize, size))
      case CubeInfo(cube, maxWeight, _) =>
        (revision.createCubeId(cube), NormalizedWeight(maxWeight))
    }.toMap
  }

  /**
   * Returns the set of cubes that are overflowed for a given space revision
   *
   * @return the set of overflowed cubes
   */

  def overflowedSet: Set[CubeId] = {
    revisionState
      .filter(_.maxWeight != Weight.MaxValue)
      .map(cubeInfo => revision.createCubeId(cubeInfo.cube))
      .toSet
  }

  /**
   * Returns the index state for the given space revision
   * @return Dataset containing cube information
   */
  def revisionState: Seq[CubeInfo] = {

    val spark = SparkSession.active
    import spark.implicits._

    val weightValueTag = TagUtils.maxWeight + ".value"

    revisionFiles
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
      .collect()
  }

}
