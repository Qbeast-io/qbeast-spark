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
 * @param tableId
 *   QTableID
 * @param indexedTableFactory
 *   configuration of the indexed table
 */
class QbeastTable private (
    sparkSession: SparkSession,
    val tableId: QTableId,
    indexedTableFactory: IndexedTableFactory)
    extends Serializable
    with StagingUtils {

  private val deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableId.id)

  private def qbeastSnapshot: DeltaQbeastSnapshot = {
    val snapshot = deltaLog.update()
    DeltaQbeastSnapshot(snapshot)
  }

  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableId)

  private def checkRevisionAvailable(revisionId: RevisionId): Unit = {
    if (!qbeastSnapshot.existsRevision(revisionId)) {
      throw AnalysisExceptionFactory.create(
        s"Revision $revisionId does not exists. " +
          s"The latest revision available is $latestRevisionId")
    }
  }

  /**
   * Returns all available revisions in the table.
   */
  def allRevisions(): Seq[Revision] = qbeastSnapshot.loadAllRevisions

  /**
   * Returns all available RevisionIds in the table.
   */
  def allRevisionIds(): Seq[RevisionId] = allRevisions().map(_.revisionId)

  /**
   * Returns the revision with the given RevisionId.
   * @param revisionId
   *   RevisionId
   */
  def revision(revisionId: RevisionId): Revision = {
    checkRevisionAvailable(revisionId)
    qbeastSnapshot.loadRevision(revisionId)
  }

  def latestRevision: Revision = qbeastSnapshot.loadLatestRevision

  def latestRevisionId: RevisionId = latestRevision.revisionId

  /**
   * Returns the indexing columns of the given revision.
   * @param revisionId
   *   RevisionId
   */
  def indexedColumns(revisionId: RevisionId): Seq[String] = {
    checkRevisionAvailable(revisionId)
    qbeastSnapshot
      .loadRevision(revisionId)
      .columnTransformers
      .map(_.columnName)
  }

  def indexedColumns(): Seq[String] =
    latestRevision.columnTransformers.map(_.columnName)

  /**
   * Returns the desired cube size of the given revision.
   * @param revisionId
   *   RevisionId
   */
  def cubeSize(revisionId: RevisionId): Int = {
    checkRevisionAvailable(revisionId)
    qbeastSnapshot.loadRevision(revisionId).desiredCubeSize
  }

  def cubeSize(): Int = latestRevision.desiredCubeSize

  /**
   * Optimizes a given index revision up to a given fraction.
   * @param revisionId
   *   the identifier of revision to optimize
   * @param fraction
   *   the fraction of the index to optimize; this value should be between (0, 1].
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimize(revisionId: RevisionId, fraction: Double, options: Map[String, String]): Unit = {
    if (!isStaging(revisionId)) {
      checkRevisionAvailable(revisionId)
      OptimizeTableCommand(revisionId, fraction, indexedTable, options).run(sparkSession)
    }
  }

  def optimize(revisionId: RevisionId, fraction: Double): Unit = {
    optimize(revisionId, fraction, Map.empty[String, String])
  }

  def optimize(revisionId: RevisionId, options: Map[String, String]): Unit = {
    optimize(revisionId, 1.0, options)
  }

  def optimize(revisionId: RevisionId): Unit = {
    optimize(revisionId, 1.0, Map.empty[String, String])
  }

  def optimize(fraction: Double, options: Map[String, String]): Unit = {
    optimize(latestRevisionId, fraction, options)
  }

  def optimize(fraction: Double): Unit = {
    optimize(latestRevisionId, fraction, Map.empty[String, String])
  }

  def optimize(options: Map[String, String]): Unit = {
    optimize(latestRevisionId, 1.0, options)
  }

  def optimize(): Unit = {
    optimize(latestRevisionId, 1.0, Map.empty[String, String])
  }

  /**
   * Optimizes the data stored in the index files specified by paths relative to the table
   * directory.
   *
   * @param files
   *   the index files to optimize
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimize(files: Seq[String], options: Map[String, String]): Unit =
    indexedTable.optimize(files, options)

  def optimize(files: Seq[String]): Unit =
    optimize(files, Map.empty[String, String])

  /**
   * Gather an overview of the index for a given revision
   * @param revisionId
   *   RevisionId
   * @param indexFiles
   *   Dataset of IndexFile
   * @return
   *   IndexMetrics
   */
  def getIndexMetrics(revisionId: RevisionId, indexFiles: Dataset[IndexFile]): IndexMetrics = {
    val denormalizedBlock = DenormalizedBlock.buildDataset(indexFiles)
    IndexMetrics(revision(revisionId), denormalizedBlock)
  }

  /**
   * Gather an overview of the index for the given RevisionId
   * @return
   *   IndexMetrics
   */
  def getIndexMetrics(revisionId: RevisionId): IndexMetrics = {
    val indexFiles = qbeastSnapshot.loadIndexFiles(revisionId)
    getIndexMetrics(revisionId, indexFiles)
  }

  def getIndexMetrics: IndexMetrics = {
    getIndexMetrics(latestRevisionId)
  }

  /**
   * Gather a dataset containing all DenormalizedBlocks for a given revision.
   *
   * @param revisionId
   *   RevisionId
   * @param indexFiles
   *   Dataset of IndexFile
   * @return
   *   Dataset of DenormalizedBlock
   */
  def getDenormalizedBlocks(
      revisionId: RevisionId,
      indexFiles: Dataset[IndexFile]): Dataset[DenormalizedBlock] = {
    DenormalizedBlock.buildDataset(indexFiles)
  }

  def getDenormalizedBlocks(revisionId: RevisionId): Dataset[DenormalizedBlock] = {
    val indexFiles = qbeastSnapshot.loadIndexFiles(revisionId)
    getDenormalizedBlocks(revisionId, indexFiles)
  }

  def getDenormalizedBlocks: Dataset[DenormalizedBlock] = {
    getDenormalizedBlocks(latestRevisionId)
  }

}

object QbeastTable {

  def forPath(spark: SparkSession, path: String): QbeastTable = {
    new QbeastTable(spark, new QTableId(path), QbeastContext.indexedTableFactory)
  }

}
