/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.TagColumns
import io.qbeast.IISeq
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Dataset

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot
 *   the internal Delta Lakes log snapshot
 */
case class DeltaQbeastSnapshot(protected override val snapshot: Snapshot)
    extends QbeastSnapshot
    with DeltaStagingUtils {

  /**
   * The current state of the snapshot.
   *
   * @return
   */
  override def isInitial: Boolean = snapshot.version == -1

  private val metadataMap: Map[String, String] = snapshot.metadata.configuration

  /**
   * Constructs revision dictionary
   *
   * @return
   *   a map of revision identifier and revision
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
   * Returns last available revision identifier
   *
   * @return
   *   revision identifier
   */
  private val lastRevisionID: RevisionID =
    metadataMap.getOrElse(MetadataConfig.lastRevisionID, "-1").toLong

  /**
   * Looks up for a revision with a certain identifier
   *
   * @param revisionID
   *   the ID of the revision
   * @return
   *   revision information for the corresponding identifier
   */
  private def getRevision(revisionID: RevisionID): Revision = {
    revisionsMap
      .getOrElse(
        revisionID,
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))
  }

  /**
   * Returns true if a revision with a specific revision identifier exists
   *
   * @param revisionID
   *   the identifier of the revision
   * @return
   *   boolean
   */
  def existsRevision(revisionID: RevisionID): Boolean = {
    revisionsMap.contains(revisionID)
  }

  /**
   * Obtains the latest IndexStatus for the last RevisionID
   *
   * @return
   *   the latest IndexStatus for lastRevisionID
   */
  override def loadLatestIndexStatus: IndexStatus = {
    loadIndexStatus(lastRevisionID)
  }

  /**
   * Obtains the latest IndexStatus for a given RevisionID
   *
   * @param revisionID
   *   the RevisionID
   * @return
   */
  override def loadIndexStatus(revisionID: RevisionID): IndexStatus = {
    val revision = getRevision(revisionID)
    new IndexStatusBuilder(this, revision).build()
  }

  override def loadLatestIndexFiles: Dataset[IndexFile] = loadIndexFiles(lastRevisionID)

  override def loadIndexFiles(revisionId: RevisionID): Dataset[IndexFile] = {
    val revision = loadRevision(revisionId)
    val dimensionCount = revision.transformations.size
    val addFiles =
      if (isStaging(revision)) loadStagingBlocks()
      else loadRevisionBlocks(revisionId)

    import addFiles.sparkSession.implicits._
    addFiles.map(IndexFiles.fromAddFile(dimensionCount))
  }

  /**
   * Obtain all Revisions for a given QTableID
   *
   * @return
   *   an immutable Seq of Revision for qTable
   */
  override def loadAllRevisions: IISeq[Revision] =
    revisionsMap.values.toVector

  /**
   * Obtain the last Revisions
   *
   * @return
   *   an immutable Seq of Revision for qTable
   */
  override def loadLatestRevision: Revision = {
    getRevision(lastRevisionID)
  }

  /**
   * Obtain the IndexStatus for a given RevisionID
   *
   * @param revisionID
   *   the RevisionID
   * @return
   *   the IndexStatus for revisionID
   */
  override def loadRevision(revisionID: RevisionID): Revision = {
    getRevision(revisionID)
  }

  /**
   * Loads the most updated revision at a given timestamp
   *
   * @param timestamp
   *   the timestamp in Long format
   * @return
   *   the latest Revision at a concrete timestamp
   */
  override def loadRevisionAt(timestamp: Long): Revision = {
    val candidateRevisions = revisionsMap.values.filter(_.timestamp <= timestamp)
    if (candidateRevisions.nonEmpty) candidateRevisions.maxBy(_.timestamp)
    else {
      throw AnalysisExceptionFactory
        .create(s"No space revision available before $timestamp")
    }
  }

  /**
   * Loads the dataset of qbeast blocks for a given revision
   * @param revisionID
   *   the revision identifier
   * @return
   *   the Dataset of QbeastBlocks
   */
  def loadRevisionBlocks(revisionID: RevisionID): Dataset[AddFile] = {
    if (isStaging(revisionID)) loadStagingBlocks()
    else snapshot.allFiles.where(TagColumns.revision === lit(revisionID.toString))
  }

  /**
   * Loads Staging AddFiles
   */
  def loadStagingBlocks(): Dataset[AddFile] = stagingFiles()
}
