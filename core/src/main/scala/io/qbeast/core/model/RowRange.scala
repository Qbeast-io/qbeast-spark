/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Range of rows specified by two indexes: from and to.
 *
 * @param from the first row index (inclusive)
 * @param to the last row index (exclusive)
 */
final case class RowRange(from: Long, to: Long) extends Serializable {
  require(0 <= from && from <= to)

  /**
   * Returns whether the rane is empty.
   */
  def isEmpty: Boolean = from == to

  /**
   * Returns the range length.
   */
  def length: Long = to - from

  /**
   * Returns whether the range contains a given value.
   *
   * @param value the value to check
   */
  def contains(value: Long): Boolean = from <= value && value < to
}
