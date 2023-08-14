/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vectorized.ColumnarArray
import org.apache.spark.sql.vectorized.ColumnarMap
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * ColumnVector implementation which wraps the target instance and applies the
 * specified range of rows.
 *
 * @param target the target instance
 * @param from the first rowId
 * @param to the last (excluded) rowId
 */
private[sources] class ColumnVectorSlice(target: ColumnVector, from: Int, to: Int)
    extends ColumnVector(target.dataType()) {
  require(from <= to)

  override def close(): Unit = target.close()

  override def hasNull(): Boolean = numNulls() > 0

  override def numNulls(): Int = (from until to).count(target.isNullAt)

  override def isNullAt(rowId: Int): Boolean = target.isNullAt(targetRowId(rowId))

  override def getBoolean(rowId: Int): Boolean = target.getBoolean(targetRowId(rowId))

  override def getByte(rowId: Int): Byte = target.getByte(targetRowId(rowId))

  override def getShort(rowId: Int): Short = target.getShort(targetRowId(rowId))

  override def getInt(rowId: Int): Int = target.getInt(targetRowId(rowId))

  override def getLong(rowId: Int): Long = target.getLong(targetRowId(rowId))

  override def getFloat(rowId: Int): Float = target.getFloat(targetRowId(rowId))

  override def getDouble(rowId: Int): Double = target.getDouble(targetRowId(rowId))

  override def getArray(rowId: Int): ColumnarArray = target.getArray(targetRowId(rowId))

  override def getMap(ordinal: Int): ColumnarMap = target.getMap(targetRowId(ordinal))

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    target.getDecimal(targetRowId(rowId), precision, scale)

  override def getUTF8String(rowId: Int): UTF8String = target.getUTF8String(targetRowId(rowId))

  override def getBinary(rowId: Int): Array[Byte] = target.getBinary(targetRowId(rowId))

  override def getChild(ordinal: Int): ColumnVector = target.getChild(targetRowId(ordinal))

  private def targetRowId(rowId: Int): Int = from + rowId
}
