/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

/**
 * Placeholder for all Delta Reading Utils
 * Delta has several methods and classes that contains Data Skipping hints
 * If you want to extend the fuctionalities, use this object
 */
object DeltaReadingUtils {

  private lazy val spark = SparkSession.active

  def useStats: Boolean = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)
}
