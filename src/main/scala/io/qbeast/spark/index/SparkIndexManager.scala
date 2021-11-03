/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{IndexManager, IndexStatus, TableChanges}

class SparkIndexManager[DataFrame] extends IndexManager[DataFrame] {
  override def index(data: DataFrame, indexStatus: IndexStatus): TableChanges = _

  override def optimize(data: DataFrame, indexStatus: IndexStatus): TableChanges = _
}
