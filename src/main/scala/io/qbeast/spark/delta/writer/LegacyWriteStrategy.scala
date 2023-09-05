/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.IndexFile
import io.qbeast.spark.index.QbeastColumns
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import io.qbeast.IISeq

/**
 * Legacy write startegy.
 */
private[writer] object LegacyWriteStrategy extends WriteStrategy {

  override def write(
      data: DataFrame,
      write: Iterator[InternalRow] => (IndexFile, TaskStats)): IISeq[(IndexFile, TaskStats)] =
    data
      .repartition(col(QbeastColumns.cubeColumnName))
      .queryExecution
      .executedPlan
      .execute
      .mapPartitions(rows => Iterator(write(rows)))
      .collect()
      .toIndexedSeq

}
