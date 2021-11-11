/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.context.QbeastContext
import io.qbeast.model.{CubeId, IndexManager, IndexStatus, TableChanges}
import org.apache.spark.sql.DataFrame

class SparkIndexManager extends IndexManager[DataFrame] {

  // TODO change this to a single implementation
  lazy val oTreeAlgorithm = QbeastContext.oTreeAlgorithm

  override def index(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) = {
    oTreeAlgorithm.index(data, indexStatus)
  }

  override def optimize(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) = {
    oTreeAlgorithm.replicateCubes(data, indexStatus)
  }

  override def analyze(indexStatus: IndexStatus): IISeq[CubeId] =
    oTreeAlgorithm.analyzeIndex(indexStatus)

}
