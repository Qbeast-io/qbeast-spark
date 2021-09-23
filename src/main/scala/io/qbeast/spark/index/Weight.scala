/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.spark.index.Weight.{MaxValue, MinValue, offset, range}

/**
 * Weight companion object.
 */
object Weight {

  /**
   * The maximum value.
   */
  val MaxValue: Weight = Weight(Int.MaxValue)

  /**
   * The minimum value.
   */
  val MinValue: Weight = Weight(Int.MinValue)

  private val offset: Double = MinValue.value.toDouble
  private val range: Double = MaxValue.value.toDouble - offset

  /**
   * Creates a weight from a given fraction. The fraction must
   * belong to [0, 1].
   *
   * @param fraction the fraction
   * @return a weight
   */
  def apply(fraction: Double): Weight = Weight((fraction * range + offset).toInt)

}

/**
 * Weight is used by OTree index cubes to define the fraction of the elements
 * stored in the cube.
 */
case class Weight(value: Int) extends Ordered[Weight] with Serializable {

  /**
   * Returns the fraction, that is the fraction of all possible
   * weight values less or equal to this. The returned value
   * belongs to [0, 1].
   *
   * @return the fraction.
   */
  def fraction: Double = (value - offset) / range

  /**
   * Adds a given weight.
   *
   * @param other the other weight
   * @return the result weight
   */
  def +(other: Weight): Weight = Weight(fraction + other.fraction)

  /**
   * Subtracts a given weight.
   *
   * @param other the other weight
   * @return the result weight
   */
  def -(other: Weight): Weight = Weight(fraction - other.fraction)

  /**
   * Multiplies by o given weight. The fraction of the returned weight
   * is a product of the fractions.
   *
   * @param other the other weight
   * @return the result weight
   */
  def *(other: Weight): Weight = Weight(fraction * other.fraction)

  /**
   * Divides by a given weight. The fraction of the returned weight
   * which is a quotient of the fractions.
   *
   * @param other the other weight
   * @return the result weight
   */
  def /(other: Weight): Weight = if (other != MinValue) {
    Weight(fraction / other.fraction)
  } else {
    MaxValue
  }

  override def compare(that: Weight): Int = value.compare(that.value)

  override def hashCode(): Int = value.hashCode()

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Weight]

  override def equals(obj: Any): Boolean = obj match {
    case Weight(otherValue) => value == otherValue
    case _ => false
  }

}
