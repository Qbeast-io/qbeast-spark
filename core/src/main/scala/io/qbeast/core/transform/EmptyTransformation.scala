package io.qbeast.core.transform

import io.qbeast.IISeq

/**
 * An empty Transformation meant for empty revisions
 */
case class EmptyTransformation() extends Transformation {

  override def transform(value: Any): Double = 0d

  override def isSupersededBy(newTransformation: Transformation): Boolean = true

  override def merge(other: Transformation): Transformation = other

  override def percentiles: IISeq[Any] = Nil

  override def transformWithPercentiles(value: Any): Double = 0d
}
