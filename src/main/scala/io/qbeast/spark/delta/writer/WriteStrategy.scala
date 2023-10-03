/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.DataFrame

/**
 * Write strategy abstraction allows to implement different approaches to the
 * writing of indexed data.
 */
private[writer] trait WriteStrategy {

  /**
   * Writes given indexed data and returns the written files with the task
   * stats.
   *
   * @param data the indexed data to write
   * @return the written files with the task stats
   */
  def write(data: DataFrame): IISeq[(IndexFile, TaskStats)]

}
