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
    sparkSession: SparkSession,
    val tableID: QTableID,
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

  private def checkRevisionAvailable(revisionID: RevisionID): Unit = {
    if (!qbeastSnapshot.existsRevision(revisionID)) {
      throw AnalysisExceptionFactory.create(
        s"Revision $revisionID does not exists. " +
          s"The latest revision available is $latestRevisionID")
    }
  }

  def allRevisions(): Seq[Revision] = qbeastSnapshot.loadAllRevisions

  def allRevisionIDs(): Seq[RevisionID] = allRevisions().map(_.revisionID)

  def revision(revisionID: RevisionID): Revision = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID)
  }

  private def latestRevision: Revision = qbeastSnapshot.loadLatestRevision

  private def latestRevisionID: RevisionID = latestRevision.revisionID

  def indexedColumns(revisionID: RevisionID): Seq[String] = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot
      .loadRevision(revisionID)
      .columnTransformers
      .map(_.columnName)
  }

  def indexedColumns(): Seq[String] =
    latestRevision.columnTransformers.map(_.columnName)

  def cubeSize(revisionID: RevisionID): Int = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID).desiredCubeSize
  }

  def cubeSize(): Int = latestRevision.desiredCubeSize

  /**
   * Optimizes a given index revision up to a given fraction.
   * @param revisionID
   *   the identifier of revision to optimize
   * @param fraction
   *   the fraction of the index to optimize; this value should be between (0, 1].
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimize(revisionID: RevisionID, fraction: Double, options: Map[String, String]): Unit = {
    if (!isStaging(revisionID)) {
      checkRevisionAvailable(revisionID)
      OptimizeTableCommand(revisionID, fraction, indexedTable, options).run(sparkSession)
    }
  }

  def optimize(revisionID: RevisionID, fraction: Double): Unit = {
    optimize(revisionID, fraction, Map.empty[String, String])
  }

  def optimize(revisionID: RevisionID, options: Map[String, String]): Unit = {
    optimize(revisionID, 1.0, options)
  }

  def optimize(revisionID: RevisionID): Unit = {
    optimize(revisionID, 1.0, Map.empty[String, String])
  }

  def optimize(fraction: Double, options: Map[String, String]): Unit = {
    optimize(latestRevisionID, fraction, options)
  }

  def optimize(fraction: Double): Unit = {
    optimize(latestRevisionID, fraction, Map.empty[String, String])
  }

  def optimize(options: Map[String, String]): Unit = {
    optimize(latestRevisionID, 1.0, options)
  }

  def optimize(): Unit = {
    optimize(latestRevisionID, 1.0, Map.empty[String, String])
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
   * Gather an overview of the index for a given revision
   * @param revisionID
   *   optional RevisionID
   * @return
   */
  def getIndexMetrics(revisionID: RevisionID): IndexMetrics = {
    val indexStatus = qbeastSnapshot.loadIndexStatus(revisionID)
    val indexFiles = qbeastSnapshot.loadIndexFiles(revisionID)
    val revision = indexStatus.revision
    val cubeStatuses = indexStatus.cubesStatuses
    val denormalizedBlock = DenormalizedBlock.buildDataset(revision, cubeStatuses, indexFiles)

    IndexMetrics(revision, denormalizedBlock)
  }

  def getIndexMetrics: IndexMetrics = {
    getIndexMetrics(latestRevisionID)
  }

  /**
   * Gather a dataset containing all the important information about the index structure.
   *
   * @param revisionID
   *   optional RevisionID
   * @return
   */
  def getDenormalizedBlocks(revisionID: RevisionID): Dataset[DenormalizedBlock] = {
    val indexStatus = qbeastSnapshot.loadIndexStatus(revisionID)
    val indexFiles = qbeastSnapshot.loadIndexFiles(revisionID)
    val revision = indexStatus.revision
    val cubeStatuses = indexStatus.cubesStatuses
    DenormalizedBlock.buildDataset(revision, cubeStatuses, indexFiles)
  }

  def getDenormalizedBlocks: Dataset[DenormalizedBlock] = {
    getDenormalizedBlocks(latestRevisionID)
  }

}

object QbeastTable {

  def forPath(spark: SparkSession, path: String): QbeastTable = {
    new QbeastTable(spark, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}
