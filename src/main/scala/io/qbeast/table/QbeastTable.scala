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
package io.qbeast.table

import io.qbeast.context.QbeastContext
import io.qbeast.core.model._
import io.qbeast.internal.commands.OptimizeTableCommand
import io.qbeast.spark.utils.IndexMetrics
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

  private def qbeastSnapshot: QbeastSnapshot = QbeastContext.metadataManager.loadSnapshot(tableID)

  /**
   * The IndexedTable representation of the Table
   * @return
   */
  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableID)

  /**
   * Checks if the revision is available in the table.
   *
   * If the revision is not available, or is not the staging revision, an exception is thrown.
   * @param revisionID
   *   the revision to check
   */
  private def checkRevisionAvailable(revisionID: RevisionID): Unit = {
    if (revisionID != stagingID && !qbeastSnapshot.existsRevision(revisionID)) {
      throw AnalysisExceptionFactory.create(
        s"Revision $revisionID does not exists. " +
          s"The latest revision available is $latestRevisionID")
    }
  }

  /**
   * Returns all available revisions in the table.
   */
  def allRevisions(): Seq[Revision] = qbeastSnapshot.loadAllRevisions

  /**
   * Returns all available RevisionIDs in the table.
   */
  def allRevisionIDs(): Seq[RevisionID] = allRevisions().map(_.revisionID)

  /**
   * Returns the revision with the given revisionID.
   * @param revisionID
   *   RevisionID
   */
  def revision(revisionID: RevisionID): Revision = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID)
  }

  def latestRevision: Revision = qbeastSnapshot.loadLatestRevision

  def latestRevisionID: RevisionID = latestRevision.revisionID

  /**
   * Returns the indexing columns of the given revision.
   * @param revisionID
   *   RevisionID
   */
  def indexedColumns(revisionID: RevisionID): Seq[String] = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot
      .loadRevision(revisionID)
      .columnTransformers
      .map(_.columnName)
  }

  def indexedColumns(): Seq[String] =
    latestRevision.columnTransformers.map(_.columnName)

  /**
   * Returns the desired cube size of the given revision.
   * @param revisionID
   *   RevisionID
   */
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
    checkRevisionAvailable(revisionID)
    OptimizeTableCommand(revisionID, fraction, indexedTable, options).run(sparkSession)
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
   * @param options
   *   Optimization options where user metadata and pre-commit hooks are specified.
   */
  def optimize(files: Seq[String], options: Map[String, String]): Unit =
    indexedTable.optimizeIndexFiles(files, options)

  def optimize(files: Seq[String]): Unit =
    optimize(files, Map.empty[String, String])

  /**
   * Gather an overview of the index for a given revision
   * @param revisionID
   *   RevisionID
   * @param indexFiles
   *   Dataset of IndexFile
   * @return
   *   IndexMetrics
   */
  def getIndexMetrics(revisionID: RevisionID, indexFiles: Dataset[IndexFile]): IndexMetrics = {
    val denormalizedBlock = DenormalizedBlock.buildDataset(indexFiles)
    IndexMetrics(revision(revisionID), denormalizedBlock)
  }

  /**
   * Gather an overview of the index for the given RevisionID
   * @return
   *   IndexMetrics
   */
  def getIndexMetrics(revisionID: RevisionID): IndexMetrics = {
    val indexFiles = qbeastSnapshot.loadIndexFiles(revisionID)
    getIndexMetrics(revisionID, indexFiles)
  }

  def getIndexMetrics: IndexMetrics = {
    getIndexMetrics(latestRevisionID)
  }

  /**
   * Gather a dataset containing all DenormalizedBlocks for a given revision.
   *
   * @param revisionID
   *   RevisionID
   * @param indexFiles
   *   Dataset of IndexFile
   * @return
   *   Dataset of DenormalizedBlock
   */
  def getDenormalizedBlocks(
      revisionID: RevisionID,
      indexFiles: Dataset[IndexFile]): Dataset[DenormalizedBlock] = {
    DenormalizedBlock.buildDataset(indexFiles)
  }

  def getDenormalizedBlocks(revisionID: RevisionID): Dataset[DenormalizedBlock] = {
    val indexFiles = qbeastSnapshot.loadIndexFiles(revisionID)
    getDenormalizedBlocks(revisionID, indexFiles)
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
