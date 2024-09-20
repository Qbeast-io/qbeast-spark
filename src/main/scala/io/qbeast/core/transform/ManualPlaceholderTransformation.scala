package io.qbeast.core.transform

/**
 * An Placeholder for Transformations that are not yet initialized
 *
 * This Placeholder is NEVER MEANT to be used as a final transformation When tryng to transform a
 * value with the transform method, it will throw an UnsupportedOperationException
 */
case class ManualPlaceholderTransformation(columnName: String, columnStatsNames: Seq[String])
    extends Transformation {

  override def transform(value: Any): Double = {
    throw new UnsupportedOperationException(
      s"ManualPlaceholderTransformation does not support transform. " +
        s"Please provide the valid transformation of $columnName through option 'columnStats'")
  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = true

  override def merge(other: Transformation): Transformation = other
}
