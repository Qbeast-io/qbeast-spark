/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

/**
 * Double value transformation.
 */
trait Transformation {

  /**
   * Converts a real number to a normalized value.
   *
   * @param value a real number to convert
   * @return a real number between 0 and 1
   */
  def transform(value: Double): Double

  /**
   * The transform and revert operation are lossy, so use this method only
   * for testing/debugging.
   *
   * @param normalizedValue a number between 0 and 1
   * @return the approximation of the original value
   */
  @deprecated(
    "Transform and revert are lossy, use this method only for testing or debugging ",
    "0.1.0")
  def revert(normalizedValue: Double): Double

}

/**
 * Identity transformation.
 */
object IdentityTransformation extends Transformation {

  @inline
  override def transform(value: Double): Double = value

  @inline
  override def revert(normalizedValue: Double): Double = normalizedValue

}

/**
 * Linear transformation from [min,max] to [0,1].
 *
 * @param min min value of a coordinate
 * @param max max value of a coordinate
 */
case class LinearTransformation(min: Double, max: Double) extends Transformation {
  require(max > min, "Range cannot be not null, and max must be > min")
  val scale: Double = 1.0f / (max - min)

  override def transform(value: Double): DimensionLog = (value - min) * scale

  override def revert(normalizedValue: Double): DimensionLog = normalizedValue / scale + min

}
