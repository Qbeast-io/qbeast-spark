/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.core.model.RowRange
import org.apache.spark.sql.vectorized.ColumnarBatch
import scala.collection.mutable.Queue

/**
 * Iterator implementation that decorates a given batch iterator by applying
 * the specified ranges of rows. This iterator does not return batches if
 * no ranges are specified.
 */
private[sources] class RangedColumnarBatchIterator(
    batches: Iterator[ColumnarBatch],
    ranges: Seq[RowRange])
    extends Iterator[ColumnarBatch] {

  private val batches_ =
    if (batches.isInstanceOf[BufferedIterator[ColumnarBatch]]) {
      batches.asInstanceOf[BufferedIterator[ColumnarBatch]]
    } else {
      batches.buffered
    }

  private var offset = 0
  private var batchToClose: Option[ColumnarBatch] = None
  private val ranges_ = Queue(ranges.filterNot(_.isEmpty).sortBy(_.from): _*)

  override def hasNext: Boolean = {
    // Skip the ranges before the first batch
    while (ranges_.nonEmpty && ranges_.front.to <= offset) {
      ranges_.dequeue()
    }
    // There are no ranges available
    if (ranges_.isEmpty) {
      return false
    }
    // Skip the batches before the first range
    while (batches_.hasNext && offset + batches_.head.numRows() <= ranges_.front.from) {
      val batch = batches_.next()
      offset += batch.numRows()
      batchToClose = Some(batch)
    }
    // The next batch if exists intersects with the first range
    val result = batches_.hasNext
    // If there are no more batches, close the used batch to free the resources
    // For unknown reasons Spark reuses the returned columnar batch so the close
    // operation must be called only for the last batch when there is no more data
    // otherwise a closed batch causes NPE in OnHeapColumnVector.
    if (!result && batchToClose.isDefined) {
      batchToClose.get.close()
    }
    result
  }

  override def next(): ColumnarBatch = {
    val batch = batches_.head
    val range = ranges_.front
    val from = math.max(offset, range.from)
    val to = math.min(offset + batch.numRows(), range.to)
    val slice = ColumnarBatchSlice(batch, (from - offset).toInt, (to - offset).toInt)
    // The batch is over
    if (offset + batch.numRows() == to) {
      offset += batch.numRows()
      batches_.next()
    }
    // The range is over
    if (range.to == to) {
      ranges_.dequeue()
    }
    slice
  }

}
