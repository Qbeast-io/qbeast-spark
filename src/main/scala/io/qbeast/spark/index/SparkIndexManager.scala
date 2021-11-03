/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{IndexManager, IndexStatus, TableChanges}
import org.apache.spark.sql.DataFrame

class SparkIndexManager extends IndexManager[DataFrame] {
  override def index(data: DataFrame, indexStatus: IndexStatus): TableChanges = null

  override def optimize(data: DataFrame, indexStatus: IndexStatus): TableChanges = null
}
