/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.StagingUtils
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.table._
import io.qbeast.spark.utils.CubeSizeMetrics
import io.qbeast.spark.utils.IndexMetrics
import io.qbeast.spark.utils.MathOps.depthOnBalance
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.AnalysisException

/**
 * Class for interacting with QbeastTable at a user level
 *
 * @param sparkSession active SparkSession
 * @param tableID      QTableID
 * @param indexedTableFactory configuration of the indexed table
 */
class QbeastTable private (
    sparkSession: SparkSession,
    tableID: QTableID,
    indexedTableFactory: IndexedTableFactory)
    extends Serializable
    with StagingUtils {

  private def deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableID.id)

  private def qbeastSnapshot: DeltaQbeastSnapshot =
    delta.DeltaQbeastSnapshot(deltaLog.snapshot)

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
   * Optimizes a given revision of the index. Optimization includes data
   * cobe data replication cube file compaction. If the revision is staging then
   * no operation.
   *
   * @param revisionID the revision identifier
   * @throws AnalysisException if the specified revision does not exist
   */
  @throws(classOf[AnalysisException])
  def optimize(revisionID: RevisionID): Unit = {
    checkRevisionAvailable(revisionID)
    if (!isStaging(revisionID)) {
      indexedTable.analyze(revisionID)
      indexedTable.replicate(revisionID)
    }
    indexedTable.compact(revisionID)
  }

  /**
   * Optimizes the latest non-staging revision of the index if any.
   */
  def optimize(): Unit = {
    optimize(latestRevisionAvailableID)
  }

  /**
   * Returns the indexed columns for a given revision of the table.
   *
   * @param revisionID the revision identifier
   * @return the indexed column names
   * @throws AnalysisException if the specified revision does not exist
   */
  @throws(classOf[AnalysisException])
  def indexedColumns(revisionID: RevisionID): Seq[String] = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot
      .loadRevision(revisionID)
      .columnTransformers
      .map(_.columnName)
  }

  /**
   * Returns the indexed columns for the latest revision of the table.
   *
   * @return the indexed column names
   */
  def indexedColumns(): Seq[String] = {
    indexedColumns(latestRevisionAvailableID)
  }

  /**
   * Returns the preferred cube size for a given revision of the table.
   *
   * @param revisionID the revision identifier
   * @return the preferred cube size
   * @throws AnalysisException if the specified revision does not exist
   */
  @throws(classOf[AnalysisException])
  def cubeSize(revisionID: RevisionID): Int = {
    checkRevisionAvailable(revisionID)
    qbeastSnapshot.loadRevision(revisionID).desiredCubeSize

  }

  /**
   * Returns the prefrred cube size for the latest table revision.
   */
  def cubeSize(): Int =
    latestRevisionAvailable.desiredCubeSize

  /**
   * Returns the revision identifiers.
   *
   * @return the revision identifiers
   */
  def revisionsIDs(): Seq[RevisionID] = {
    qbeastSnapshot.loadAllRevisions.map(_.revisionID)
  }

  /**
   * Returns the latest revision identifier.
   *
   * @return the latest revision identifier
   */
  def latestRevisionID(): RevisionID = {
    latestRevisionAvailableID
  }

  /**
   * Returns the index metrics for a given index revision. If the revision
   * identifier is None, then the latest revision is used.
   *
   * @param revisionID optional revision identifier
   * @return the index metrics
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
    val elementCount = cubeStatuses.flatMap(_._2.files.map(_.elementCount)).sum

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

  /**
   * Creates a new QbeastTable instance for given Spark session and path.
   *
   * @param spark the spark session
   * @param path the path
   * @return a new instance
   */
  def forPath(spark: SparkSession, path: String): QbeastTable = {
    new QbeastTable(spark, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}
