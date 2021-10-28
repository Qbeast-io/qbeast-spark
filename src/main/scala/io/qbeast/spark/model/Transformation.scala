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

}

/**
 * Identity transformation.
 */
object IdentityTransformation extends Transformation {

  @inline
  override def transform(value: Double): Double = value

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

}
