/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats}

case class TaskStats(writeTaskStats: Seq[WriteTaskStats], endTime: Long)

object StatsTracker {

  private var statsTrackers: Seq[WriteJobStatsTracker] = Seq.empty

  def registerStatsTrackers(newStatsTrackers: Seq[WriteJobStatsTracker]): Unit = {
    statsTrackers = newStatsTrackers
  }

  def getStatsTrackers(): Seq[WriteJobStatsTracker] = statsTrackers

}
