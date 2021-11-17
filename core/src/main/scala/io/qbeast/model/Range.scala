package io.qbeast.model

/**
 * A range of values.
 * @param from the lower bound of the range
 * @param to the upper bound of the range
 * @tparam T the type of the range
 */
case class Range[T](from: T, to: T)
