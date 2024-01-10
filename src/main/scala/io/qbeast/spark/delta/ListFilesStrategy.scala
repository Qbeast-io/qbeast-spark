/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory

/**
 * List files strategy abstraction allows to implement different approaches to compute the set of
 * files contributing to the query results.
 */
private[delta] trait ListFilesStrategy {

  /**
   * Lists the files from the given index applying the provided filters.
   *
   * @param target
   *   the target index
   * @param partitionFilters
   *   the partition filters
   * @param dataFilters
   *   the data filters
   * @return
   *   a sequence of partition directories
   */
  def listFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory]

}
