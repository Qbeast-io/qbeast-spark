/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.{QTableID, RevisionID}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.table._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import io.qbeast.spark.internal.commands.AnalyzeTableCommand
import io.qbeast.spark.internal.commands.OptimizeTableCommand

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

  private def getAvailableRevision(revisionID: Option[RevisionID]): RevisionID = {
    revisionID match {
      case Some(id) if qbeastSnapshot.existsRevision(id) =>
        id
      case None => qbeastSnapshot.loadLatestRevision.revisionID
    }
  }

  /**
   * The optimize operation should read the data of those cubes announced
   * and replicate it in their children
   * @param revisionID the identifier of the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   */
  def optimize(revisionID: Option[RevisionID] = None): Unit = {
    OptimizeTableCommand(getAvailableRevision(revisionID), indexedTable)
      .run(sparkSession)

  }

  /**
   * The analyze operation should analyze the index structure
   * and find the cubes that need optimization
   * @param revisionID the identifier of the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   * @return the sequence of cubes to optimize in string representation
   */
  def analyze(revisionID: Option[RevisionID] = None): Seq[String] = {
    AnalyzeTableCommand(getAvailableRevision(revisionID), indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

  def getIndexMetrics(revisionID: Option[RevisionID] = None): IndexMetrics = {
    val allCubeStatuses = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

    val cubeCounts = allCubeStatuses.size
    val depth = allCubeStatuses.map(_._1.depth).max
    val row_count = allCubeStatuses.flatMap(_._2.files.map(_.elementCount)).sum
    val dimensionCount = allCubeStatuses.toList.head._1.dimensionCount
    val desiredCubeSize = qbeastSnapshot.loadLatestRevision.desiredCubeSize

    val nonLeafStatuses =
      allCubeStatuses.filter(_._1.children.exists(allCubeStatuses.contains)).values
    val nonLeafCubeSizes = nonLeafStatuses.flatMap(_.files.map(_.size)).toSeq.sorted
    val nonLeafCubeSizeDeviation =
      nonLeafCubeSizes
        .map(cubeSize => math.pow(cubeSize - desiredCubeSize, 2) / nonLeafCubeSizes.size)
        .sum

    val avgFanOut = nonLeafStatuses
      .map(_.cubeId.children.count(allCubeStatuses.contains))
      .sum / nonLeafStatuses.size

    val depthOverLogNumNodes = depth / logOfBase(dimensionCount, cubeCounts)
    val depthOnBalance = depth / logOfBase(dimensionCount, row_count / desiredCubeSize)

    IndexMetrics(
      dimensionCount,
      row_count,
      depth,
      cubeCounts,
      desiredCubeSize,
      avgFanOut,
      depthOverLogNumNodes,
      depthOnBalance,
      NonLeafCubeSizeDetails(
        nonLeafCubeSizes.min,
        nonLeafCubeSizes((nonLeafCubeSizes.size * 0.25).toInt),
        nonLeafCubeSizes((nonLeafCubeSizes.size * 0.50).toInt),
        nonLeafCubeSizes((nonLeafCubeSizes.size * 0.75).toInt),
        nonLeafCubeSizes.max,
        nonLeafCubeSizeDeviation))
  }

  def logOfBase(base: Int, value: Double): Double = {
    math.log10(value) / math.log10(base)
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
    dev: Double) {

  override def toString: String = {
    s"""Non-lead Cube Size Stats:
       |- min: $min
       |- firstQuartile: $firstQuartile
       |- secondQuartile: $secondQuartile
       |- thirdQuartile: $thirdQuartile
       |- max: $max
       |- dev: $dev
       |""".stripMargin
  }

}

case class IndexMetrics(
    dimensionCount: Int,
    row_count: Long,
    depth: Int,
    cubeCounts: Int,
    desiredCubeSize: Int,
    avgFanOut: Double,
    depthOverLogNumNodes: Double,
    depthOnBalance: Double,
    nonLeafCubeSizeDetails: NonLeafCubeSizeDetails) {

  override def toString: String = {
    s"""OTree Index Metrics:
       |dimensionCount: $dimensionCount
       |row_count: $row_count
       |depth: $depth
       |cubeCounts: $cubeCounts
       |desiredCubeSize: $desiredCubeSize
       |avgFanOut: $avgFanOut
       |depthOverLogNumNodes: $depthOverLogNumNodes
       |depthOnBalance: $depthOnBalance
       |$nonLeafCubeSizeDetails
       |""".stripMargin
  }

}
