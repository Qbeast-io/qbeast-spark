/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.transform

import io.qbeast.model.OrderedDataType

case class LinearTransformation(minNumber: Any, maxNumber: Any, orderedDataType: OrderedDataType)
    extends Transformation {

  import orderedDataType.ordering._

  private val mn = minNumber.toDouble

  val scale: Double = {
    val mx = maxNumber.toDouble
    require(mx > mn, "Range cannot be not null, and max must be > min")
    1.0 / (mx - mn)
  }

  override def transform(value: Any): Double = {
    (value.toDouble - mn) * scale
  }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   * @return a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = {
    other match {
      case LinearTransformation(otherMin, otherMax, otherOrdering)
          if orderedDataType == otherOrdering =>
        LinearTransformation(min(minNumber, otherMin), max(maxNumber, otherMax), orderedDataType)
          .asInstanceOf[Transformation]
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation the new transformation created with statistics over the new data
   * @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case LinearTransformation(newMin, newMax, otherOrdering)
          if orderedDataType == otherOrdering =>
        gt(minNumber, newMin) || gt(maxNumber, newMax)
    }

}
