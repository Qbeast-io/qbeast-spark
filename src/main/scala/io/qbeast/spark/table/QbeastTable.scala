/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.spark.context.QbeastContext
import io.qbeast.spark.sql.qbeast.QbeastSnapshot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Class for interacting with QbeastTable at a user level
 *
 * @param sparkSession active SparkSession
 * @param path path of the table
 * @param indexedTableFactory configuration of the indexed table
 */
class QbeastTable private (
    sparkSession: SparkSession,
    path: String,
    indexedTableFactory: IndexedTableFactory)
    extends Serializable {

  private def deltaLog: DeltaLog = DeltaLog.forTable(sparkSession, indexedTable.path)

  private def qbeastSnapshot: QbeastSnapshot =
    QbeastSnapshot(deltaLog.snapshot, QbeastContext.config.getInt("qbeast.index.size"))

  private def indexedTable: IndexedTable =
    indexedTableFactory.getIndexedTable(sparkSession.sqlContext, path)

  private def getAvailableRevision(revisionTimestamp: Option[Long]): Long = {
    revisionTimestamp match {
      case Some(timestamp) if qbeastSnapshot.existsRevision(timestamp) => timestamp
      case None => qbeastSnapshot.lastRevisionTimestamp
    }
  }

  /**
   * The optimize operation should read the data of those cubes announced
   * and replicate it in their children
   * @param revisionTimestamp the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   */
  def optimize(revisionTimestamp: Option[Long] = None): Unit = {
    OptimizeTableCommand(getAvailableRevision(revisionTimestamp), indexedTable)
      .run(sparkSession)

  }

  /**
   * The analyze operation should analyze the index structure
   * and find the cubes that need optimization
   * @param revisionTimestamp the revision to optimize.
   *                          If doesn't exist or none is specified, would be the last available
   * @return the sequence of cubes to optimize
   */
  def analyze(revisionTimestamp: Option[Long] = None): Seq[String] = {
    AnalyzeTableCommand(getAvailableRevision(revisionTimestamp), indexedTable)
      .run(sparkSession)
      .map(_.getString(0))
  }

}

object QbeastTable {

  def forPath(sparkSession: SparkSession, path: String): QbeastTable = {
    new QbeastTable(sparkSession, path, QbeastContext.indexedTableFactory)
  }

}
