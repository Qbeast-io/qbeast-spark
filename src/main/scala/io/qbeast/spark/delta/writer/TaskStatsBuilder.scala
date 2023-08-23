/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker

/**
 * Builder for collecting and assembling task stats while writing an index file.
 *
 * @param path the path of the index file being written
 * @param trackers the stats tracker to collect the stats
 */
private[writer] class TaskStatsBuilder(path: String, trackers: Seq[WriteTaskStatsTracker]) {

  /**
   * Updates the stats after the file is opened.
   *
   * @return this instance
   */
  def fileOpened(): TaskStatsBuilder = {
    trackers.foreach(_.newFile(path))
    this
  }

  /**
   * Updates the stats after a given row is written.
   *
   * @return this instance
   */
  def rowWritten(row: InternalRow): TaskStatsBuilder = {
    trackers.foreach(_.newRow(path, row))
    this
  }

  /**
   * Updates the stats after the file is written.
   *
   * @return this instance
   */
  def fileWritten(): TaskStatsBuilder = {
    trackers.foreach(_.closeFile(path))
    this
  }

  /**
   * Builds the task stats.
   *
   * @return the tast stats
   */
  def result(): TaskStats = {
    val time = System.currentTimeMillis()
    TaskStats(trackers.map(_.getFinalStats(time)), time)
  }

}
