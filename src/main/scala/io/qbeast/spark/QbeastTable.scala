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
    val allCubeSizes = allCubeStatuses.flatMap(_._2.files.map(_.size))
    val cubeCounts = allCubeStatuses.size
    val maxCubeSize = allCubeSizes.max
    val depth = allCubeStatuses.map(_._1.depth).max
    val row_count = allCubeStatuses.flatMap(_._2.files.map(_.elementCount)).sum
    val dimensionCount = allCubeStatuses.toList.head._1.dimensionCount
    val desiredCubeSize = qbeastSnapshot.loadLatestRevision.desiredCubeSize

    val nonLeafStatuses =
      allCubeStatuses.filter(_._1.children.exists(allCubeStatuses.contains)).values
    val nonLeafCubeSizes = nonLeafStatuses.flatMap(_.files.map(_.size)).toSeq
    val nonLeafCubeSizeDeviation =
      nonLeafCubeSizes
        .map(cubeSize => math.pow(cubeSize - desiredCubeSize, 2) / nonLeafCubeSizes.size)
        .sum

    val fanOut = nonLeafStatuses.map(_.cubeId.children.count(allCubeStatuses.contains)).toSeq

    val depthOverLogNumNodes = depth / logOfBase(dimensionCount, cubeCounts)
    val depthOnBalance = depth / logOfBase(dimensionCount, row_count / desiredCubeSize)

    val details = NonLeafCubeSizeDetails(
      nonLeafCubeSizes.min,
      nonLeafCubeSizes((nonLeafCubeSizes.size * 0.25).toInt),
      nonLeafCubeSizes((nonLeafCubeSizes.size * 0.50).toInt),
      nonLeafCubeSizes((nonLeafCubeSizes.size * 0.75).toInt),
      nonLeafCubeSizes.max,
      nonLeafCubeSizeDeviation)

    val metadata = Metadata(
      dimensionCount,
      row_count,
      depth,
      cubeCounts,
      maxCubeSize,
      desiredCubeSize,
      fanOut,
      depthOverLogNumNodes,
      depthOnBalance)

    val cubeSizes = CubeSizes(allCubeSizes, nonLeafCubeSizes, details)
    IndexMetrics(cubeSizes, metadata)
  }

  def logOfBase(base: Int, value: Double): Double = {
    math.log10(value) / math.log10(base)
  }

  case class IndexMetrics(cubeSizes: CubeSizes, metadata: Metadata)

  case class CubeSizes(
      allCubeSizes: Iterable[Long],
      nonLeafCubeSizes: Iterable[Long],
      nonLeafCubeSizeDetails: NonLeafCubeSizeDetails)

  case class Metadata(
      dimensionCount: Int,
      row_count: Long,
      depth: Int,
      cubeCounts: Int,
      maxCubeSize: Long,
      desiredCubeSize: Int,
      fanOut: Seq[Int],
      depthOverLogNumNodes: Double,
      depthOnBalance: Double)

  case class NonLeafCubeSizeDetails(
      min: Long,
      firstQuartile: Long,
      secondQuartile: Long,
      thirdQuartile: Long,
      max: Long,
      dev: Double)

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(sparkSession, new QTableID(path), QbeastContext.indexedTableFactory)
  }

}
