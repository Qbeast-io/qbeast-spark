/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.qbeast.IISeq
import io.qbeast.core.transform.Transformer.percentiles

import java.sql.{Date, Timestamp}

/**
 * Double value transformation.
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.CLASS,
  include = JsonTypeInfo.As.PROPERTY,
  property = "className")
trait Transformation extends Serializable {

  /**
   * Converts a real number to a normalized value.
   *
   * @param value a real number to convert
   * @return a real number between 0 and 1
   */
  def transform(value: Any): Double

  def percentiles: IISeq[Any]

  def transformWithPercentiles(value: Any): Double

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation the new transformation created with statistics over the new data
   * @return true if the domain of the newTransformation is not fully contained in this one.
   */
  def isSupersededBy(newTransformation: Transformation): Boolean

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @return a new Transformation that contains both this and other.
   */
  def merge(other: Transformation): Transformation
}

trait OrdinalTransformation extends Transformation {
  def ordering: Ordering[Any]

}

object Transformation {
  val maxPos: Int = percentiles.length - 1

  def fractionMapping(v: Double, percentileValues: IISeq[Double]): Double = {
    var i = 0
    while (i < maxPos && percentileValues(i + 1) < v) {
      i += 1
    }

    var j = maxPos
    while (0 < j && percentileValues(j - 1) > v) {
      j -= 1
    }

    val (x0, x1) = (percentiles(i), percentiles(j))
    val (y0, y1) = (percentileValues(i), percentileValues(j))

    if (y0 == y1) x0
    else {
      val m = (y1 - y0) / (x1 - x0)
      (v - y0) / m + x0
    }
  }

}

/**
 * Identity transformation.
 */
case class IdentityToZeroTransformation(identityValue: Any) extends Transformation {

  @inline
  override def transform(value: Any): Double = value match {

    case v: Number if v == identityValue => 0.0
    case v: Timestamp if v == identityValue => 0.0
    case v: Date if v == identityValue => 0.0

  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this

  override def percentiles: IISeq[Any] = Nil

  override def transformWithPercentiles(value: Any): Double = 0d
}

/**
 * Zero value for nulls transformation.
 */
object NullToZeroTransformation extends Transformation {

  @inline
  override def transform(value: Any): Double = value match {
    case null => 0.0
  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this

  override def percentiles: IISeq[Any] = Nil

  override def transformWithPercentiles(value: Any): Double = 0d
}
