/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model.{
  CubeId,
  IndexStatus,
  NormalizedWeight,
  ReplicatedSet,
  Revision,
  RevisionID,
  Weight,
  mapper
}
import io.qbeast.spark.index.writer.BlockStats
import io.qbeast.spark.utils.{MetadataConfig, State, TagUtils}
import org.apache.spark.sql.{AnalysisExceptionFactory, Dataset, SparkSession}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions.{min, sum}

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 */
case class QbeastSnapshot(snapshot: Snapshot) {

  /**
   * Returns the sequence of blocks for a set of cubes belonging to a specific space revision
   * @param cubes the set of cubes
   * @return the sequence of blocks
   */
  def getCubeBlocks(indexStatus: IndexStatus, cubes: Set[CubeId]): Seq[AddFile] = {
    val revision = indexStatus.revision
    val files =
      snapshot.allFiles.filter(_.tags(TagUtils.revision) == revision.revisionID.toString)
    files
      .filter(_.tags(TagUtils.state) != State.ANNOUNCED)
      .filter(a => cubes.contains(revision.createCubeId(a.tags(TagUtils.cube))))
      .collect()
  }

  def isInitial: Boolean = snapshot.version == -1

  private val metadataMap: Map[String, String] = snapshot.metadata.configuration

  /**
   * Constructs revision dictionary
   *
   * @return a map of revision identifier and revision
   */
  private val revisionsMap: Map[RevisionID, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key: String, json: String) =>
      val revisionID = key.split('.').last.toLong
      val revision = mapper
        .readValue[Revision](json, classOf[Revision])
      (revisionID, revision)
    }
  }

  /**
   * Constructs replicated set for each revision
   *
   * @return a map of revision identifier and replicated set
   */
  private val replicatedSetsMap: Map[RevisionID, ReplicatedSet] = {
    val listReplicatedSets = metadataMap.filterKeys(_.startsWith(MetadataConfig.replicatedSet))

    listReplicatedSets.map { case (key: String, json: String) =>
      val revisionID = key.split('.').last.toLong
      val revision = getRevision(revisionID)
      val replicatedSet = mapper
        .readValue[Set[String]](json, classOf[Set[String]])
        .map(revision.createCubeId)
      (revisionID, replicatedSet)
    }
  }

  /**
   * Returns last available revision identifier
   *
   * @return revision identifier
   */
  val lastRevisionID: RevisionID =
    metadataMap.getOrElse(MetadataConfig.lastRevisionID, "-1").toLong

  /**
   * Returns available revisions
   *
   * @return a sequence of revisions
   */
  def revisions: IISeq[Revision] = revisionsMap.values.toVector

  /**
   * Looks up for a revision with a certain identifier
   *
   * @param revisionID the ID of the revision
   * @return revision information for the corresponding identifier
   */
  def getRevision(revisionID: RevisionID): Revision = {
    revisionsMap
      .getOrElse(
        revisionID,
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))
  }

  /**
   * Looks up for the revision data of certain revision identifier
   *
   * @param revision instance
   * @return revision information for the corresponding identifier
   */
  def getRevisionData(revision: Revision): IndexStatus = {
    new DeltaSnapshotIndexStatusBuilder(revision).build()
  }

  /**
   * Looks up for the revision data of certain revision identifier
   *
   * @param revisionID the ID of the revision
   * @return revision information for the corresponding identifier
   */
  def getIndexStatus(revisionID: RevisionID): IndexStatus = {
    val revision = getRevision(revisionID)
    new DeltaSnapshotIndexStatusBuilder(revision).build()
  }

  /**
   * Looks up for the revision data of certain revision identifier
   *
   * @param revision the revision we want to analyze.
   * @return revision information for the corresponding identifier
   */
  def getIndexStatus(revision: Revision): IndexStatus = {
    new DeltaSnapshotIndexStatusBuilder(revision).build()
  }

  /**
   * Returns the last available revision data
   *
   * @return the revision data
   */
  def lastRevisionData: IndexStatus = {
    getIndexStatus(lastRevisionID)
  }

  def lastRevision: Revision = getRevision(lastRevisionID)

  /**
   * Returns true if a revision with a specific revision identifier exists
   *
   * @param revisionID the identifier of the revision
   * @return boolean
   */
  def existsRevision(revisionID: RevisionID): Boolean = {
    revisionsMap.contains(revisionID)
  }

  /**
   * Snapshot that provides information about the current index state of a given Revision.
   * @param revision the given Revision
   * @param replicatedSet the set of replicated cubes for the specific Revision
   * @param files dataset of AddFiles that belongs to the specific Revision
   */
  private[delta] class DeltaSnapshotIndexStatusBuilder(
      revision: Revision,
      announcedSet: Set[CubeId] = Set.empty)
      extends Serializable {

    def revisionFiles: Dataset[AddFile] = {
      // this must be external to the lambda, to avoid SerializationErrors
      val revID = revision.revisionID.toString
      snapshot.allFiles.filter(_.tags(TagUtils.revision) == revID)
    }

    def replicatedSet: ReplicatedSet =
      replicatedSetsMap.getOrElse(revision.revisionID, Set.empty)

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
     *
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
    }

  }

}

/**
 * Cube Information
 *
 * @param cube      Id of the cube
 * @param maxWeight Maximum weight of the cube
 * @param size      Number of elements of the cube
 */

private[delta] case class CubeInfo(cube: String, maxWeight: Weight, size: Long)
