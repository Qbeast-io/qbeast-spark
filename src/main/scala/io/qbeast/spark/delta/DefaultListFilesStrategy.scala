/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory

/**
 * Default implementation of the ListFilesStrategy which simply applies Delta filtering based on
 * min/max and other stats.
 */
private[delta] object DefaultListFilesStrategy extends ListFilesStrategy with Serializable {

  override def listFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] =
    target.listFiles(partitionFilters, dataFilters)

}
