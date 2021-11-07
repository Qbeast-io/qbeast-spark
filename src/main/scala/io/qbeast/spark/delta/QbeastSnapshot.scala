/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.IISeq
import io.qbeast.model.{Revision, RevisionID, mapper}
import io.qbeast.spark.index.ReplicatedSet
import io.qbeast.spark.utils.{MetadataConfig, TagUtils}
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.delta.Snapshot

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 */
case class QbeastSnapshot(snapshot: Snapshot) {

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
   * @param revisionID the ID of the revision
   * @return revision information for the corresponding identifier
   */
  def getRevisionData(revisionID: RevisionID): DeltaIndexStatus = {
    val revision = getRevision(revisionID)
    val replicatedSet = replicatedSetsMap.getOrElse(revisionID, Set.empty)
    val revisionFiles = snapshot.allFiles.filter(_.tags(TagUtils.revision) == revisionID.toString)
    DeltaIndexStatus(revision, replicatedSet, revisionFiles)
  }

  /**
   * Returns the last available revision data
   *
   * @return the revision data
   */
  def lastRevisionData: DeltaIndexStatus = {
    getRevisionData(lastRevisionID)
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

}
