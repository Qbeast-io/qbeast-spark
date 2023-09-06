/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import org.apache.spark.sql.catalyst.InternalRow
import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.execution.datasources.OutputWriter

/**
 * Writer to write a single index file.
 *
 * @param writer the output writer
 * @param fileBuilder the index file builder
 * @param statsBuilder the task stats builder
 * @param excludeQbeastColumns the function to exclude the Qbeast-specific
 * columns
 */
private[writer] class IndexFileWriter(
    writer: OutputWriter,
    fileBuilder: IndexFileBuilder,
    statsBuilder: TaskStatsBuilder,
    excludeQbeastColumns: InternalRow => InternalRow) {

  var closed: Boolean = false
  statsBuilder.fileOpened()

  /**
   * Writes a given row which must have Qbeast-specific columns like weight and
   * target cube identifier.
   *
   * @param row the row to write
   */
  def write(extendedRow: InternalRow): Unit = {
    if (closed) {
      throw new IllegalStateException("Writer is already closed")
    }
    val row = excludeQbeastColumns(extendedRow)
    writer.write(row)
    fileBuilder.rowWritten(extendedRow)
    statsBuilder.rowWritten(extendedRow)
  }

  /**
   * Closes the writer and returns the written index file with the task stats.
   *
   * @return the index file and the task stats
   */
  def close(): (IndexFile, TaskStats) = {
    if (!closed) {
      writer.close()
      fileBuilder.fileWritten()
      statsBuilder.fileWritten()
      closed = true
    }
    (fileBuilder.result(), statsBuilder.result())
  }

}
