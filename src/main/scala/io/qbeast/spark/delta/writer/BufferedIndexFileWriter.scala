/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.Weight
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable.Buffer
import scala.collection.mutable.Map

/**
 * Implementation of IndexFileWriter that uses buffers to reduce the number of
 * cube blocks in case when the rows are written out of order.
 *
 * @param target the target index file writer
 * @param limit the maximum number of rows of a single cube to buffer
 */
private[writer] class BufferedIndexFileWriter(target: IndexFileWriter, limit: Int)
    extends IndexFileWriter {

  private val buffers = Map.empty[CubeId, Buffer[(InternalRow, Weight)]]

  override def write(row: InternalRow, cubeId: CubeId, weight: Weight): Unit = {
    val buffer = buffers.getOrElseUpdate(cubeId, Buffer.empty)
    buffer.append((row, weight))
    if (buffer.size >= limit) {
      flush(cubeId, buffer)
      buffers.remove(cubeId)
    }
  }

  override def close(): (IndexFile, TaskStats) = {
    buffers.foreach { case (cubeId, buffer) => flush(cubeId, buffer) }
    buffers.clear()
    target.close()
  }

  private def flush(cubeId: CubeId, buffer: Buffer[(InternalRow, Weight)]): Unit = {
    buffer.foreach { case (row, weight) =>
      target.write(row, cubeId, weight)
    }
  }

}
