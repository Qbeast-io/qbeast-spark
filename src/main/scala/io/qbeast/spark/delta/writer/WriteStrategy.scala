/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import io.qbeast.IISeq

/**
 * Write strategy is responsible for grouping the rows of a given indexed
 * DataFrame and for writing them with the provided index file writer.
 */
private[writer] trait WriteStrategy {

  /**
   * Writes given indexed data, i.e. data where each row is assigned the target
   * cube which is stored in the dedicated column. It uses a given write
   * function to write individual index files. The write function must be
   * serializable.
   *
   * The strategy is responsible for deciding how many files to write and how to
   * distribute the rows between them.
   *
   * @param data the index data to write
   * @param write the index data to write
   * @return the written index files and stats
   */
  def write(
      data: DataFrame,
      write: Iterator[InternalRow] => (IndexFile, TaskStats)): IISeq[(IndexFile, TaskStats)]

}
