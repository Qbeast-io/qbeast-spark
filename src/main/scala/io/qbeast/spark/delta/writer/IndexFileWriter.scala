/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.Weight
import org.apache.spark.sql.catalyst.InternalRow

/**
 * Index file writer writes the indexed data row by row and finally returns the
 * written index file with the task stats while being closed.
 */
private[writer] trait IndexFileWriter {

  /**
   * Writes a given row taking into account the provided target cube and the
   * weight.
   *
   * @param row the (original, i.e. without Qbeast-specific fields) row to write
   * @param cubeId the identifier of the cube assigned to the row
   * @param weight the row weight
   */
  def write(row: InternalRow, cubeId: CubeId, weight: Weight): Unit

  /**
   * Closes the writer and returns the written index file with the task stats.
   *
   * @return the written index file with the task stats
   */
  def close(): (IndexFile, TaskStats)

}
