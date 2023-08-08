/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Range represent a continous part of a sequnce contained between the specified
 * indexes.
 *
 * @param from the starting index (inclusive)
 * @param to the finishing index (exclusive)
 */
final case class Range(from: Long, to: Long) extends Serializable {
  require(0 <= from && from <= to)

  /**
   * Returns whether the rane is empty.
   */
  def isEmpty: Boolean = from == to

  /**
   * Returns the range length.
   */
  def length: Long = to - from
}
