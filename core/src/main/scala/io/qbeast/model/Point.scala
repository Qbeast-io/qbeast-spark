/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.model

import io.qbeast.IISeq

/**
 * Point companion object.
 */
object Point {

  /**
   * Creates a point from given coordinates.
   *
   * @param coordinates the coordinates
   * @return a point
   */
  def apply(coordinates: Double*): Point = apply(coordinates.toIndexedSeq)
}

/**
 * Point in a multidimensional space.
 *
 * @param coordinates the coordinates
 */
case class Point(coordinates: IISeq[Double]) {
  require(coordinates.nonEmpty)

  /**
   * Returns the dimension count.
   *
   * @return the dimension count
   */
  def dimensionCount: Int = coordinates.length

  /**
   * Scales the point by given factors.
   *
   * @param factors the factors
   * @return the scaled point
   */
  def scale(factors: Seq[Double]): Point =
    transform[Double](factors, (x: Double, y: Double) => x * y)

  /**
   * Scales the point by a given factor.
   *
   * @param factor the factor
   * @return the scaled point
   */
  def scale(factor: Double): Point = transform(factor, (x: Double, y: Double) => x * y)

  /**
   * Moves the point by given shifts.
   *
   * @param shifts the shifts
   * @return the moved point
   */
  def move(shifts: Seq[Double]): Point =
    transform[Double](shifts, (x: Double, y: Double) => x + y)

  /**
   * Moves the point by a given shift.
   *
   * @param shift the shift
   * @return the moved point
   */
  def move(shift: Double): Point = transform[Double](shift, (x: Double, y: Double) => x + y)

  /**
   * Returns whether all the coordinates of this point are less than
   * corresponding coordinates of a given one.
   *
   * @param that that point
   * @return all coordinates are less than corresponding coordinates
   *         of the other point
   */
  def <(that: Point): Boolean = {
    require(that.dimensionCount == dimensionCount)
    coordinates.zip(that.coordinates).forall { case (c1, c2) => c1 < c2 }
  }

  /**
   * Returns whether all the coordinates of this point are less than
   * or equal to corresponding coordinates of a given one.
   *
   * @param that that point
   * @return all coordinates are less than or equal to corresponding
   *         coordinates of the other point
   */
  def <=(that: Point): Boolean = {
    require(that.dimensionCount == dimensionCount)
    coordinates.zip(that.coordinates).forall { case (c1, c2) => c1 <= c2 }
  }

  override def hashCode(): Int = {
    coordinates.foldLeft(0) { (value, coordinate) =>
      31 * value + java.lang.Double.hashCode(coordinate)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: Point =>
      dimensionCount == that.dimensionCount && coordinates.zip(that.coordinates).forall {
        case (c1, c2) => c1 == c2
      }
    case _ => false
  }

  override def toString: String = s"Point($coordinates)"

  private def transform[S](seeds: Seq[S], operation: (Double, S) => Double): Point = {
    require(seeds.length == dimensionCount)
    Point(coordinates.zip(seeds).map { case (c, s) => operation(c, s) })
  }

  private def transform[S](seed: S, operation: (Double, S) => Double): Point = Point(
    coordinates.map(operation(_, seed)))

}
