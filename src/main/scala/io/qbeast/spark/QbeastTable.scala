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
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.StagingUtils
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.commands.AnalyzeTableCommand
import io.qbeast.spark.internal.commands.CompactTableCommand
import io.qbeast.spark.internal.commands.OptimizeTableCommand
import io.qbeast.spark.table._
import io.qbeast.spark.utils.CubeSizeMetrics
import io.qbeast.spark.utils.IndexMetrics
import io.qbeast.spark.utils.MathOps.depthOnBalance
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.AnalysisExceptionFactory
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
    tableID: QTableID,
    indexedTableFactory: IndexedTableFactory)
    extends Serializable
    with StagingUtils {

  private def deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableID.id)

  private def qbeastSnapshot: DeltaQbeastSnapshot =
    delta.DeltaQbeastSnapshot(deltaLog.update())

  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableID)

  private def latestRevisionAvailable = qbeastSnapshot.loadLatestRevision

  private def latestRevisionAvailableID = latestRevisionAvailable.revisionID

  private def checkRevisionAvailable(revisionID: RevisionID): Unit = {
    if (!qbeastSnapshot.existsRevision(revisionID)) {
      throw AnalysisExceptionFactory.create(
        s"Revision $revisionID does not exists. " +
          s"The latest revision available is $latestRevisionAvailableID")
    }
  }

  /**
   * The optimize operation should read the data of those cubes announced and replicate it in
   * their children
   * @param revisionID
   *   the identifier of the revision to optimize. If doesn't exist or none is specified, would be
   *   the last available
   */
  def optimize(revisionID: RevisionID): Unit = {
    checkRevisionAvailable(revisionID)
    OptimizeTableCommand(revisionID, indexedTable)
      .run(sparkSession)

  }

  def optimize(): Unit = {
    optimize(latestRevisionAvailableID)

  }

  /**
   * Optimizes the data stored in the index files specified by paths relative to the table
   * directory.
   *
   * @param files
   *   the index files to optimize
   */
  def optimize(files: Seq[String]): Unit = indexedTable.optimize(files)

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
    if (isStaging(latestRevisionAvailableID)) Seq.empty
    else analyze(latestRevisionAvailableID)
  }

  /**
   * The compact operation should compact the small files in the table
   * @param revisionID
   *   the identifier of the revision to optimize. If doesn't exist or none is specified, would be
   *   the last available
   */
  def compact(revisionID: RevisionID): Unit = {
    checkRevisionAvailable(revisionID)
    CompactTableCommand(revisionID, indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

  def compact(): Unit = {
    compact(latestRevisionAvailableID)
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

  def indexedColumns(): Seq[String] = {
    latestRevisionAvailable.columnTransformers.map(_.columnName)
  }

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

  def cubeSize(): Int =
    latestRevisionAvailable.desiredCubeSize

  /**
   * Outputs all the revision identifiers available for the table
   * @return
   */
  def revisionsIDs(): Seq[RevisionID] = {
    qbeastSnapshot.loadAllRevisions.map(_.revisionID)
  }

  /**
   * Outputs the identifier of the latest revision available
   * @return
   */
  def latestRevisionID(): RevisionID = {
    latestRevisionAvailableID
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

    val revision = indexStatus.revision
    val cubeStatuses = indexStatus.cubesStatuses

    val cubeCount = cubeStatuses.size
    val depth = if (cubeCount == 0) -1 else cubeStatuses.map(_._1.depth).max + 1
    val elementCount = cubeStatuses.flatMap(_._2.blocks.map(_.elementCount)).sum

    val indexingColumns = revision.columnTransformers.map(_.columnName)
    val dimensionCount = indexingColumns.size
    val desiredCubeSize = revision.desiredCubeSize

    val innerCs = cubeStatuses.filterKeys(_.children.exists(cubeStatuses.contains))

    val avgFanout = if (innerCs.nonEmpty) {
      innerCs.keys.toSeq
        .map(_.children.count(cubeStatuses.contains))
        .sum / innerCs.size.toDouble
    } else 0d

    IndexMetrics(
      cubeStatuses,
      dimensionCount,
      elementCount,
      depth,
      cubeCount,
      desiredCubeSize,
      indexingColumns.mkString(","),
      avgFanout,
      depthOnBalance(depth, cubeCount, dimensionCount),
      CubeSizeMetrics(innerCs, desiredCubeSize))
  }

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(sparkSession, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}
