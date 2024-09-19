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
package io.qbeast.spark

import io.qbeast.context.QbeastContext
import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.commands.AnalyzeTableCommand
import io.qbeast.spark.internal.commands.OptimizeTableCommand
import io.qbeast.spark.table._
import io.qbeast.spark.utils.IndexMetrics
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

/**
 * Class for interacting with QbeastTable at a user level
 *
 * @param sparkSession
 *   active SparkSession
 * @param tableID
 *   QTableID
 * @param indexedTableFactory
 *   configuration of the indexed table
 */
class QbeastTable private (
    val tableID: QTableID,
    sparkSession: SparkSession,
    indexedTableFactory: IndexedTableFactory)
    extends Serializable
    with StagingUtils {
  private val deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableID.id)

  private var qbeastSnapshotCache: Option[DeltaQbeastSnapshot] = None

  private def qbeastSnapshot: DeltaQbeastSnapshot = {
    if (qbeastSnapshotCache.isEmpty) {
      val snapshot = deltaLog.update()
      qbeastSnapshotCache = Some(DeltaQbeastSnapshot(snapshot))
    }
    qbeastSnapshotCache.get
  }

  def update(): Unit = {
    val snapshot = deltaLog.update()
    qbeastSnapshotCache = Some(DeltaQbeastSnapshot(snapshot))
  }

  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableID)

  private def latestRevision: Revision = qbeastSnapshot.loadLatestRevision

  private def latestRevisionID: RevisionID = latestRevision.revisionID

  private def checkRevisionAvailable(revisionID: RevisionID): Unit = {
    if (!qbeastSnapshot.existsRevision(revisionID)) {
      throw AnalysisExceptionFactory.create(
        s"Revision $revisionID does not exists. " +
          s"The latest revision available is $latestRevisionID")
    }
  }

  /**
   * The optimize operation should read the data of those cubes announced and replicate it in
   * their children
   * @param revisionID
   *   the identifier of the revision to optimize. If doesn't exist or none is specified, would be
   *   the last available
   */
  def optimize(revisionID: RevisionID, options: Map[String, String]): Unit = {
    if (!isStaging(revisionID)) {
      checkRevisionAvailable(revisionID)
      OptimizeTableCommand(revisionID, indexedTable, options)
        .run(sparkSession)
    }
  }

  def optimize(revisionID: RevisionID): Unit = {
    optimize(revisionID, Map.empty[String, String])
  }

  def optimize(options: Map[String, String]): Unit = {
    optimize(latestRevisionID, options)
  }

  def optimize(): Unit = {
    optimize(Map.empty[String, String])
  }

  /**
   * Optimizes the data stored in the index files specified by paths relative to the table
   * directory.
   *
   * @param files
   *   the index files to optimize
   */
  def optimize(files: Seq[String], options: Map[String, String]): Unit =
    indexedTable.optimize(files, options)

  def optimize(files: Seq[String]): Unit =
    optimize(files, Map.empty[String, String])

  /**
   * The analyze operation should analyze the index structure and find the cubes that need
   * optimization
   * @param revisionID
   *   the identifier of the revision to optimize. If doesn't exist or none is specified, would be
   *   the last available
   * @return
   *   the sequence of cubes to optimize in string representation
   */
  def analyze(revisionID: RevisionID): Seq[String] = {
    if (isStaging(revisionID)) Seq.empty
    else {
      checkRevisionAvailable(revisionID)
      AnalyzeTableCommand(revisionID, indexedTable)
        .run(sparkSession)
        .map(_.getString(0))
    }
  }

  def analyze(): Seq[String] = {
    if (isStaging(latestRevisionID)) Seq.empty
    else analyze(latestRevisionID)
  }

  /**
   * Outputs the indexed columns of the table
   * @param revisionID
   *   the identifier of the revision. If doesn't exist or none is specified, would be the last
   *   available
   * @return
   */

  def indexedColumns(revisionID: RevisionID): Seq[String] = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot
      .loadRevision(revisionID)
      .columnTransformers
      .map(_.columnName)
  }

  /**
   * Outputs the indexed columns of the table
   * @return
   */
  def indexedColumns(): Seq[String] =
    latestRevision.columnTransformers.map(_.columnName)

  /**
   * Outputs the cubeSize of the table
   * @param revisionID
   *   the identifier of the revision. If doesn't exist or none is specified, would be the last
   *   available
   * @return
   */
  def cubeSize(revisionID: RevisionID): Int = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID).desiredCubeSize
  }

  def cubeSize(): Int = latestRevision.desiredCubeSize

  def allRevisions(): Seq[Revision] = qbeastSnapshot.loadAllRevisions

  def allRevisionIDs(): Seq[RevisionID] = allRevisions().map(_.revisionID)

  def revision(revisionID: RevisionID): Revision = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID)
  }

  /**
   * Gather an overview of the index for a given revision
   * @param revisionID
   *   optional RevisionID
   * @return
   */
  def getIndexMetrics(revisionID: Option[RevisionID] = None): IndexMetrics = {
    val indexStatus = revisionID match {
      case Some(id) => qbeastSnapshot.loadIndexStatus(id)
      case None => qbeastSnapshot.loadLatestIndexStatus
    }
    val indexFiles = revisionID match {
      case Some(id) => qbeastSnapshot.loadIndexFiles(id)
      case None => qbeastSnapshot.loadLatestIndexFiles
    }

    val revision = indexStatus.revision
    val cubeStatuses = indexStatus.cubesStatuses
    val denormalizedBlock = DenormalizedBlock.buildDataset(revision, cubeStatuses, indexFiles)

    IndexMetrics(revision, denormalizedBlock)
  }

  /**
   * Gather a dataset containing all the important information about the index structure.
   *
   * @param revisionID
   *   optional RevisionID
   * @return
   */
  def getDenormalizedBlocks(revisionID: Option[RevisionID] = None): Dataset[DenormalizedBlock] = {
    val indexStatus = revisionID match {
      case Some(id) => qbeastSnapshot.loadIndexStatus(id)
      case None => qbeastSnapshot.loadLatestIndexStatus
    }
    val indexFiles = revisionID match {
      case Some(id) => qbeastSnapshot.loadIndexFiles(id)
      case None => qbeastSnapshot.loadLatestIndexFiles
    }

    val revision = indexStatus.revision
    val cubeStatuses = indexStatus.cubesStatuses
    DenormalizedBlock.buildDataset(revision, cubeStatuses, indexFiles)

  }

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(new QTableID(path), sparkSession, QbeastContext.indexedTableFactory)
  }

}
