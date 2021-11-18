/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.qbeast.model.{IndexStatus, QueryManager}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkQueryManager extends QueryManager[SparkPlan, DataFrame] {

  // TODO Implement Query Manager/other sorts of QueryOptimization
  override def query(query: SparkPlan, indexStatus: IndexStatus): DataFrame = {
    SparkSession.active.emptyDataFrame
  }

}
