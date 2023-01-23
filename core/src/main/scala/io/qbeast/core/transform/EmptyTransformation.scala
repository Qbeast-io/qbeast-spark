package io.qbeast.core.transform

/**
 * An empty Transformation meant for empty revisions
 */
case class EmptyTransformation() extends Transformation {

  override def transform(value: Any): Double = 0d

  override def isSupersededBy(newTransformation: Transformation): Boolean = true

  override def merge(other: Transformation): Transformation = this
}
