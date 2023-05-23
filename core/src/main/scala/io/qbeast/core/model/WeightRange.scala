package io.qbeast.core.model

/**
 * A range of values.
 *
 * @param from the lower bound of the range
 * @param to the upper bound of the range
 */
case class WeightRange(from: Weight, to: Weight) {

  /**
   * Returns whether the range is empty.
   *
   * @return the range is empty
   */
  def isEmpty = from >= to
}
