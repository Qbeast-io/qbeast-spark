/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.{CubeId, CubeStatus, QTableID, RevisionID}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.commands.{AnalyzeTableCommand, OptimizeTableCommand}
import io.qbeast.spark.table._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

import scala.collection.immutable.SortedMap

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
    extends Serializable {

  private def deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, tableID.id)

  private def qbeastSnapshot: DeltaQbeastSnapshot =
    delta.DeltaQbeastSnapshot(deltaLog.snapshot)

  private def indexedTable: IndexedTable = indexedTableFactory.getIndexedTable(tableID)

  private def latestRevisionAvailable = qbeastSnapshot.loadLatestRevision

  private def latestRevisionAvailableID = latestRevisionAvailable.revisionID

  private def getAvailableRevision(revisionID: RevisionID): RevisionID = {
    if (qbeastSnapshot.existsRevision(revisionID)) revisionID else latestRevisionAvailableID
  }

  /**
   * The optimize operation should read the data of those cubes announced
   * and replicate it in their children
   * @param revisionID the identifier of the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   */
  def optimize(revisionID: RevisionID): Unit = {
    OptimizeTableCommand(getAvailableRevision(revisionID), indexedTable)
      .run(sparkSession)
  }

  def optimize(): Unit = {
    OptimizeTableCommand(latestRevisionAvailableID, indexedTable)
      .run(sparkSession)
  }

  /**
   * The analyze operation should analyze the index structure
   * and find the cubes that need optimization
   * @param revisionID the identifier of the revision to optimize.
   *                        If doesn't exist or none is specified, would be the last available
   * @return the sequence of cubes to optimize in string representation
   */
  def analyze(revisionID: RevisionID): Seq[String] = {
    AnalyzeTableCommand(getAvailableRevision(revisionID), indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

  def analyze(): Seq[String] = {
    AnalyzeTableCommand(latestRevisionAvailableID, indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

  def getIndexMetrics(revisionID: Option[RevisionID] = None): IndexMetrics = {
    val allCubeStatuses = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

    val cubeCount = allCubeStatuses.size
    val depth = allCubeStatuses.map(_._1.depth).max
    val rowCount = allCubeStatuses.flatMap(_._2.files.map(_.elementCount)).sum

    val dimensionCount = indexedColumns().size
    val desiredCubeSize = cubeSize()

    val (avgFanout, details) = getInnerCubeSizeDetails(allCubeStatuses, desiredCubeSize)

    IndexMetrics(
      allCubeStatuses,
      dimensionCount,
      rowCount,
      depth,
      cubeCount,
      desiredCubeSize,
      avgFanout,
      depthOnBalance(depth, cubeCount, dimensionCount),
      details)
  }

  private def logOfBase(base: Int, value: Double): Double = {
    math.log10(value) / math.log10(base)
  }

  private def depthOnBalance(depth: Int, cubeCount: Int, dimensionCount: Int): Double = {
    val c = math.pow(2, dimensionCount).toInt
    val theoreticalDepth = logOfBase(c, 1 - cubeCount * (1 - c)) - 1
    depth / theoreticalDepth
  }

  private def getInnerCubeSizeDetails(
      cubeStatuses: SortedMap[CubeId, CubeStatus],
      desiredCubeSize: Int): (Double, NonLeafCubeSizeDetails) = {
    val innerCubeStatuses =
      cubeStatuses.filter(_._1.children.exists(cubeStatuses.contains))
    val innerCubeSizes =
      innerCubeStatuses.values.map(_.files.map(_.elementCount).sum).toSeq.sorted
    val innerCubeCount = innerCubeSizes.size.toDouble

    val avgFanout = innerCubeStatuses.keys.toSeq
      .map(_.children.count(cubeStatuses.contains))
      .sum / innerCubeCount

    val details =
      if (innerCubeCount == 0) {
        NonLeafCubeSizeDetails(0, 0, 0, 0, 0, 0, 0, "")
      } else {
        val l1_dev = innerCubeSizes
          .map(cs => math.abs(cs - desiredCubeSize))
          .sum / innerCubeCount / desiredCubeSize

        val l2_dev = math.sqrt(
          innerCubeSizes
            .map(cs => (cs - desiredCubeSize) * (cs - desiredCubeSize))
            .sum) / innerCubeCount / desiredCubeSize

        val levelStats = "\n(level, average weight, average cube size):\n" +
          innerCubeStatuses
            .groupBy(cw => cw._1.depth)
            .mapValues { m =>
              val weights = m.values.map(_.normalizedWeight)
              val elementCounts = m.values.map(_.files.map(_.elementCount).sum)
              (weights.sum / weights.size, elementCounts.sum / elementCounts.size)
            }
            .toSeq
            .sortBy(_._1)
            .mkString("\n")

        NonLeafCubeSizeDetails(
          innerCubeSizes.min,
          innerCubeSizes((innerCubeCount * 0.25).toInt),
          innerCubeSizes((innerCubeCount * 0.50).toInt),
          innerCubeSizes((innerCubeCount * 0.75).toInt),
          innerCubeSizes.max,
          l1_dev,
          l2_dev,
          levelStats)
      }
    (avgFanout, details)
  }

  /**
   * Outputs the indexed columns of the table
   * @param revisionID the identifier of the revision.
   *                          If doesn't exist or none is specified, would be the last available
   * @return
   */

  def indexedColumns(revisionID: RevisionID): Seq[String] = {
    qbeastSnapshot
      .loadRevision(getAvailableRevision(revisionID))
      .columnTransformers
      .map(_.columnName)
  }

  def indexedColumns(): Seq[String] = {
    latestRevisionAvailable.columnTransformers.map(_.columnName)
  }

  /**
   * Outputs the cubeSize of the table
   * @param revisionID the identifier of the revision.
   *                          If doesn't exist or none is specified, would be the last available
   * @return
   */
  def cubeSize(revisionID: RevisionID): Int =
    qbeastSnapshot.loadRevision(getAvailableRevision(revisionID)).desiredCubeSize

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

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(sparkSession, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}

case class NonLeafCubeSizeDetails(
    min: Long,
    firstQuartile: Long,
    secondQuartile: Long,
    thirdQuartile: Long,
    max: Long,
    l1_dev: Double,
    l2_dev: Double,
    levelStats: String) {

  override def toString: String = {
    s"""Non-leaf Cube Size Stats:
       |Quartiles:
       |- min: $min
       |- 1stQ: $firstQuartile
       |- 2ndQ: $secondQuartile
       |- 3rdQ: $thirdQuartile
       |- max: $max
       |- l1_dev: $l1_dev
       |- l2_dev: $l2_dev
       |$levelStats
       |""".stripMargin
  }

}

case class IndexMetrics(
    cubeStatuses: Map[CubeId, CubeStatus],
    dimensionCount: Int,
    elementCount: Long,
    depth: Int,
    cubeCount: Int,
    desiredCubeSize: Int,
    avgFanout: Double,
    depthOnBalance: Double,
    nonLeafCubeSizeDetails: NonLeafCubeSizeDetails) {

  override def toString: String = {
    s"""OTree Index Metrics:
       |dimensionCount: $dimensionCount
       |elementCount: $elementCount
       |depth: $depth
       |cubeCount: $cubeCount
       |desiredCubeSize: $desiredCubeSize
       |avgFanout: $avgFanout
       |depthOnBalance: $depthOnBalance
       |\n$nonLeafCubeSizeDetails
       |""".stripMargin
  }

}
