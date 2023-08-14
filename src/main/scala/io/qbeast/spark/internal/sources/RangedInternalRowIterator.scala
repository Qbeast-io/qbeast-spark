/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.apache.spark.sql.catalyst.InternalRow
import io.qbeast.core.model.RowRange
import scala.collection.mutable.Queue

/**
 * Iterator implementation which applies the specified ranges to the underlying
 * iterator. This iterator does not return rows if no ranges are specified.
 */
private[sources] class RangedInternalRowIterator(
    rows: Iterator[InternalRow],
    ranges: Seq[RowRange])
    extends Iterator[InternalRow] {
  private val queue = Queue(ranges.filterNot(_.isEmpty).sortBy(_.from): _*)
  private var position: Long = 0

  override def hasNext: Boolean = {
    // In theory it never happens, the ranges are assumed to be disjoint
    // just in case skip the ranges before the current position
    while (queue.nonEmpty && queue.front.to <= position) {
      queue.dequeue()
    }
    // No ranges, no rows
    if (queue.isEmpty) {
      return false
    }
    // Skip the rows before the start of the front range
    while (rows.hasNext && position < queue.front.from) {
      rows.next()
      position += 1
    }
    // There is still a row to iterate
    rows.hasNext
  }

  override def next(): InternalRow = {
    val row = rows.next()
    position += 1
    // The front range is over
    if (position == queue.front.to) {
      queue.dequeue()
    }
    row
  }

}
