/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.qbeast.model.{IndexStatus, QueryManager}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkQueryManager extends QueryManager[SparkPlan, DataFrame] {

  override def query(query: SparkPlan, indexStatus: IndexStatus): DataFrame = {
    SparkSession.active.emptyDataFrame
  }

}
