/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector

/**
 * ColumnarBatch implementation which wraps the target batch and applies the
 * specified range of rows.
 *
 * @param columns the column slices
 * @param from the begin of the range
 * @param to the end of the range (exclusive)
 */
private[sources] class ColumnarBatchSlice private (
    columns: Array[ColumnVector],
    from: Int,
    to: Int)
    extends ColumnarBatch(columns, to - from) {

  override def setNumRows(numRows: Int): Unit = ()
}

/**
 * ColumnarBatchSlice companion object.
 */
private[sources] object ColumnarBatchSlice {

  /**
   * Creates a new instance for given target and range of rows.
   */
  def apply(target: ColumnarBatch, from: Int, to: Int): ColumnarBatchSlice = {
    require(0 <= from && from <= to && to <= target.numRows())
    val columns = Array.newBuilder[ColumnVector]
    for (i <- 0 until target.numCols()) {
      columns += new ColumnVectorSlice(target.column(i), from, to)
    }
    new ColumnarBatchSlice(columns.result(), from, to)
  }

}
