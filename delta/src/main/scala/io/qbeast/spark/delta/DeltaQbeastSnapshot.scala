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
import io.qbeast.spark.index.IndexStatusBuilder
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.TagColumns
import io.qbeast.IISeq
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param tableID
 *   the table ID
 */
case class DeltaQbeastSnapshot(tableID: QTableID) extends QbeastSnapshot with DeltaStagingUtils {

  override val basePath: Path = new Path(tableID.id)

  override val snapshot: Snapshot =
    DeltaLog.forTable(SparkSession.active, tableID.id).update()

  /**
   * The current state of the snapshot.
   *
   * @return
   */
  override def isInitial: Boolean = snapshot.version == -1

  override lazy val schema: StructType = snapshot.metadata.schema

  override lazy val allFilesCount: Long = snapshot.allFiles.count

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
  override def existsRevision(revisionID: RevisionID): Boolean = {
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

  override def loadIndexFiles(revisionID: RevisionID): Dataset[IndexFile] = {
    val dimensionCount = loadRevision(revisionID).transformations.size
    val addFiles =
      if (isStaging(revisionID)) loadStagingFiles()
      else snapshot.allFiles.where(TagColumns.revision === lit(revisionID.toString))
    import addFiles.sparkSession.implicits._
    addFiles.map(DeltaQbeastFileUtils.fromAddFile(dimensionCount))
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
   * Loads the dataset of qbeast blocks from index files
   * @param indexFile
   *   A dataset of index files
   * @return
   *   the Datasetframe
   */

  override def loadDataframeFromIndexFiles(indexFiles: Dataset[IndexFile]): DataFrame = {
    if (snapshot.deletionVectorsSupported) {

      // TODO find a cleaner version to get a subset of data from the parquet considering the deleted parts.
      throw new UnsupportedOperationException("Deletion vectors are not supported yet")
    } else {
      import indexFiles.sparkSession.implicits._
      val rootPath = snapshot.path.getParent
      val paths = indexFiles.map(ifile => new Path(rootPath, ifile.path).toString).collect()

      indexFiles.sparkSession.read
        .schema(snapshot.schema)
        .parquet(paths: _*)

    }
  }

  /**
   * Loads Staging AddFiles
   */
  private def loadStagingFiles(): Dataset[AddFile] = stagingFiles()

  /**
   * Lists the files present in the staging area
   * @param fileIndex
   *   FileIndex instance
   * @param partitionFilters
   *   Partition filters
   * @param dataFilters
   *   Data filters
   *
   * @return
   *   Sequence of FileStatusWithMetadata
   */
  override def listStagingAreaFiles(
      fileIndex: FileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatusWithMetadata] = {

    fileIndex
      .asInstanceOf[TahoeLogFileIndex]
      .matchingFiles(partitionFilters, dataFilters)
      .filter(isStagingFile)
      .map(file =>
        new FileStatus(file.size, false, 0, 1, file.modificationTime, getAbsolutePath(file)))
      .map(file => FileStatusWithMetadata(file, Map.empty))
  }

  private def getAbsolutePath(file: AddFile): Path = {
    val path = file.toPath
    if (path.isAbsolute) path else new Path(basePath, path)
  }

  /**
   * Loads the file index
   * @return
   *   the FileIndex
   */
  override def loadFileIndex(): FileIndex = {
    TahoeLogFileIndex(
      SparkSession.active,
      snapshot.deltaLog,
      basePath,
      snapshot,
      Seq.empty,
      isTimeTravelQuery = false)
  }

}
