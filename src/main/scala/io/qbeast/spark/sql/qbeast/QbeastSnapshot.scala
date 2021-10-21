/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.{CubeId, NormalizedWeight, ReplicatedSet, Weight}
import io.qbeast.spark.model.{CubeInfo, SpaceRevision}
import io.qbeast.spark.sql.utils.MetadataConfig
import io.qbeast.spark.sql.utils.State
import io.qbeast.spark.sql.utils.TagUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisExceptionFactory, Dataset, SparkSession}

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 */
case class QbeastSnapshot(snapshot: Snapshot) {

  def isInitial: Boolean = snapshot.version == -1

  lazy val metadataMap: Map[String, String] = snapshot.metadata.configuration

  val indexedCols: Seq[String] = metadataMap.get(MetadataConfig.indexedColumns) match {
    case Some(json) => JsonUtils.fromJson[Seq[String]](json)
    case None => Seq.empty
  }

  val desiredCubeSize: Int = metadataMap.get(MetadataConfig.desiredCubeSize) match {
    case Some(value) => value.toInt
    case None => 0
  }

  val dimensionCount: Int = indexedCols.length

  /**
   * Looks up for a space revision with a certain timestamp
   * @param revisionTimestamp the timestamp of the revision
   * @return a SpaceRevision with the corresponding timestamp
   */
  def getRevisionAt(revisionTimestamp: Long): SpaceRevision = {
    spaceRevisionsMap
      .getOrElse(
        revisionTimestamp,
        throw AnalysisExceptionFactory.create(
          s"No Revision with id $lastRevisionTimestamp is found"))
  }

  def existsRevision(revisionTimestamp: Long): Boolean = {
    spaceRevisionsMap.contains(revisionTimestamp)
  }

  /**
   * Returns the cube maximum weights for a given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return a map with key cube and value max weight
   */
  def cubeWeights(revisionTimestamp: Long): Map[CubeId, Weight] = {
    indexState(revisionTimestamp)
      .collect()
      .map(info => (CubeId(dimensionCount, info.cube), info.maxWeight))
      .toMap
  }

  /**
   * Returns the cube maximum normalized weights for a given space revision
   *
   * @param revisionTimestamp the timestamp of the revision
   * @return a map with key cube and value max weight
   */
  def cubeNormalizedWeights(revisionTimestamp: Long): Map[CubeId, Double] = {
    indexState(revisionTimestamp)
      .collect()
      .map {
        case CubeInfo(cube, Weight.MaxValue, size) =>
          (CubeId(dimensionCount, cube), NormalizedWeight(desiredCubeSize, size))
        case CubeInfo(cube, maxWeight, _) =>
          (CubeId(dimensionCount, cube), NormalizedWeight(maxWeight))
      }
      .toMap
  }

  /**
   * Returns the set of cubes that are overflowed for a given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return the set of overflowed cubes
   */

  def overflowedSet(revisionTimestamp: Long): Set[CubeId] = {
    indexState(revisionTimestamp)
      .filter(_.maxWeight != Weight.MaxValue)
      .collect()
      .map(cubeInfo => CubeId(dimensionCount, cubeInfo.cube))
      .toSet
  }

  /**
   * Returns the replicated set for a given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return the set of cubes in a replicated state
   */
  def replicatedSet(revisionTimestamp: Long): ReplicatedSet = {
    replicatedSetsMap.getOrElse(revisionTimestamp, Set.empty)
  }

  lazy val replicatedSetsMap: Map[Long, ReplicatedSet] = {
    val listReplicatedSets = metadataMap.filterKeys(_.startsWith(MetadataConfig.replicatedSet))

    listReplicatedSets.map { case (key: String, json: String) =>
      val revisionTimestamp = key.split('.').last.toLong
      val replicatedSet = JsonUtils
        .fromJson[Set[String]](json)
        .map(cube => CubeId(dimensionCount, cube))
      (revisionTimestamp, replicatedSet)
    }
  }

  /**
   * Returns available space revisions ordered by timestamp
   * @return a sequence of SpaceRevision
   */
  lazy val spaceRevisionsMap: Map[Long, SpaceRevision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))

    listRevisions
      .map { case (key: String, json: String) =>
        val revisionTimestamp = key.split('.').last.toLong
        val spaceRevision = JsonUtils
          .fromJson[SpaceRevision](json)
        (revisionTimestamp, spaceRevision)
      }
  }

  lazy val spaceRevisions: Seq[SpaceRevision] = spaceRevisionsMap.values.toSeq

  lazy val lastRevisionTimestamp: Long = metadataMap(MetadataConfig.lastRevisionTimestamp).toLong

  /**
   * Returns the space revision with the higher timestamp
   * @return the space revision
   */
  lazy val lastSpaceRevision: SpaceRevision = {
    getRevisionAt(lastRevisionTimestamp)
  }

  /**
   * Returns the index state for the given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return Dataset containing cube information
   */
  private def indexState(revisionTimestamp: Long): Dataset[CubeInfo] = {

    val spark = SparkSession.active
    import spark.implicits._

    val allFiles = snapshot.allFiles
    val weightValueTag = TagUtils.maxWeight + ".value"

    allFiles
      .filter(_.tags(TagUtils.space) == revisionTimestamp.toString)
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
   * @param revisionTimestamp the timestamp of the revision
   * @return the sequence of blocks
   */
  def getCubeBlocks(cubes: Set[CubeId], revisionTimestamp: Long): Seq[AddFile] = {
    val dimensionCount = this.dimensionCount
    snapshot.allFiles
      .filter(_.tags(TagUtils.space) == revisionTimestamp.toString)
      .filter(_.tags(TagUtils.state) != State.ANNOUNCED)
      .filter(a => cubes.contains(CubeId(dimensionCount, a.tags(TagUtils.cube))))
      .collect()
  }

}
