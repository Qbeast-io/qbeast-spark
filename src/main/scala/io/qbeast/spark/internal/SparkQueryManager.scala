/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.qbeast.model.{IndexStatus, QueryManager}

class SparkQueryManager[SparkPlan, DataFrame] extends QueryManager[SparkPlan, DataFrame] {
  override def query(query: SparkPlan, indexStatus: IndexStatus): DataFrame = { _ }
}
