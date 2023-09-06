/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.DataFrame
import io.qbeast.IISeq

/**
 * Write strategy writes a given indexed data frame, i.e. a data frame where
 * every row has an assigned weight and target cube.
 */
private[writer] trait WriteStrategy {

  /**
   * Writes a given indexed data using the specified writer factory and returns
   * the writte index files with the corresponding task stats.
   *
   * @param data the indexed data to write
   * @param writerFactory the writer factory to create writers for writing the
   * index files
   * @return the written index files and the corresponding task stats
   */
  def write(data: DataFrame, writerFactory: IndexFileWriterFactory): IISeq[(IndexFile, TaskStats)]

}
