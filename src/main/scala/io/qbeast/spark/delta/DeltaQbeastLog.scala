/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.sources.BaseRelation

/**
 * A Qbeast Delta Log.
 * @param deltaLog
 */
case class DeltaQbeastLog(deltaLog: DeltaLog) {

  def qbeastSnapshot: DeltaQbeastSnapshot =
    DeltaQbeastSnapshot(deltaLog.update())

  def createRelation: BaseRelation = deltaLog.createRelation()
}
