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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
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
   * The current table properties of the snapshot.
   *
   * We filter out the revision, leaving only Revision ID.
   *
   * @return
   */
  override def loadProperties: Map[String, String] =
    snapshot.getProperties.filterKeys(k => !k.startsWith(MetadataConfig.revision)).toMap

  /**
   * The current table description.
   * @return
   */
  override def loadDescription: String = snapshot.metadata.description

  /**
   * Constructs revision dictionary
   *
   * @return
   *   a map of revision identifier and revision
   */
  private val revisionsMap: Map[RevisionId, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key: String, json: String) =>
      val revisionId = key.split('.').last.toLong
      val revision = mapper
        .readValue[Revision](json, classOf[Revision])
      (revisionId, revision)
    }
  }

  /**
   * Returns last available revision identifier
   *
   * @return
   *   revision identifier
   */
  private val lastRevisionId: RevisionId =
    metadataMap.getOrElse(MetadataConfig.lastRevisionId, "-1").toLong

  /**
   * Looks up for a revision with a certain identifier
   *
   * @param revisionId
   *   the ID of the revision
   * @return
   *   revision information for the corresponding identifier
   */
  private def getRevision(revisionId: RevisionId): Revision = {
    revisionsMap
      .getOrElse(
        revisionId,
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionId"))
  }

  /**
   * Returns true if a revision with a specific revision identifier exists
   *
   * @param revisionId
   *   the identifier of the revision
   * @return
   *   boolean
   */
  def existsRevision(revisionId: RevisionId): Boolean = {
    revisionsMap.contains(revisionId)
  }

  /**
   * Obtains the latest IndexStatus for the last RevisionId
   *
   * @return
   *   the latest IndexStatus for lastRevisionId
   */
  override def loadLatestIndexStatus: IndexStatus = {
    loadIndexStatus(lastRevisionId)
  }

  /**
   * Obtains the latest IndexStatus for a given RevisionId
   *
   * @param revisionId
   *   the RevisionId
   * @return
   */
  override def loadIndexStatus(revisionId: RevisionId): IndexStatus = {
    val revision = getRevision(revisionId)
    new IndexStatusBuilder(this, revision).build()
  }

  override def loadLatestIndexFiles: Dataset[IndexFile] = loadIndexFiles(lastRevisionId)

  override def loadIndexFiles(revisionId: RevisionId): Dataset[IndexFile] = {
    val revision = loadRevision(revisionId)
    val dimensionCount = revision.transformations.size
    val addFiles =
      if (isStaging(revision)) loadStagingFiles()
      else loadRevisionFiles(revisionId)

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
    getRevision(lastRevisionId)
  }

  /**
   * Obtain the IndexStatus for a given RevisionId
   *
   * @param revisionId
   *   the RevisionId
   * @return
   *   the IndexStatus for revisionId
   */
  override def loadRevision(revisionId: RevisionId): Revision = {
    getRevision(revisionId)
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
   * @param revisionId
   *   the revision identifier
   * @return
   *   the Dataset of QbeastBlocks
   */
  def loadRevisionFiles(revisionId: RevisionId): Dataset[AddFile] = {
    if (isStaging(revisionId)) loadStagingFiles()
    else snapshot.allFiles.where(TagColumns.revision === lit(revisionId.toString))
  }

  /**
   * Loads Staging AddFiles
   */
  def loadStagingFiles(): Dataset[AddFile] = stagingFiles()

  override def loadDataframeFromIndexFiles(indexFile: Dataset[IndexFile]): DataFrame = {
    if (snapshot.deletionVectorsSupported) {

      // TODO find a cleaner version to get a subset of data from the parquet considering the deleted parts.
      throw new UnsupportedOperationException("Deletion vectors are not supported yet")
    } else {
      import indexFile.sparkSession.implicits._
      val rootPath = snapshot.path.getParent
      val paths = indexFile.map(ifile => new Path(rootPath, ifile.path).toString).collect()

      indexFile.sparkSession.read
        .schema(snapshot.schema)
        .parquet(paths: _*)

    }
  }

}
