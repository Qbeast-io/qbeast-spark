/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

import io.qbeast.spark.index.Weight
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.DoubleType

import scala.collection.immutable.IndexedSeq

/**
 * Represents the Space where the data is fitted.
 *
 * @param timestamp moment when the revision was created
 * @param transformations transformations for each coordinate in the space
 */
case class SpaceRevision(timestamp: Long, transformations: IndexedSeq[LinearTransformation]) {

  /**
   * Transforms given coordinates.
   *
   * @param coordinates the coordinates
   * @return the point with transformed coordinates
   */
  def transform(coordinates: IndexedSeq[Double]): Point = {
    Point(transformations.zip(coordinates).map { case (trans, value) =>
      trans.transform(value)
    })
  }

  /**
   * Transforms a given point.
   *
   * @param point the point
   * @return the point with transformed coordinates
   */
  def transform(point: Point): Point = transform(point.coordinates)

  /**
   * Returns whether a given data frame fits the space revision for each index column.
   *
   * @param dataFrame the data frame
   * @param columnsToIndex the column to index
   * @return the data frame fits the space revision
   */
  def contains(dataFrame: DataFrame, columnsToIndex: Seq[String]): Boolean = {
    transformations
      .zip(ColumnInfo.get(dataFrame, columnsToIndex))
      .forall { case (transformation, info) =>
        transformation.min <= info.min && info.max <= transformation.max
      }
  }

  override def toString: String = JsonUtils.toJson(this)
}

object SpaceRevision {

  /**
   * Creates a space revision from a given data frame to index.
   *
   * @param dataFrame the data frame
   * @param columnsToIndex the columns to index
   * @return a space revision
   */
  def apply(dataFrame: DataFrame, columnsToIndex: Seq[String]): SpaceRevision = {
    val transformations = ColumnInfo
      .get(dataFrame, columnsToIndex)
      .map(info => {
        val expansion = (info.max - info.min) / 2
        LinearTransformation(info.min - expansion, info.max + expansion)
      })
      .toIndexedSeq
    new SpaceRevision(System.currentTimeMillis(), transformations)
  }

}

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

object DimensionalContext {

  def apply(numberOfDimensions: Int): DimensionalContext = {

    val charOff = numberOfDimensions match {
      case a if a <= 3 => '0'
      case a if a <= 5 => 'A'
      case 6 => '0'
      case _ => 0 // in this case we can't print
    }
    val dimSpace = 1 << numberOfDimensions
    new DimensionalContext(
      Math.log(numberOfDimensions),
      numberOfDimensions,
      dimSpace,
      charOff,
      0.until(dimSpace).map(a => (a + charOff).toChar))
  }

}

/**
 * @param dimensionLog       the ln(#dimension) to use for logarithms  in dimension space
 * @param numberOfDimensions the number of dimensions
 * @param dimSpace           the dimensional fan-out 2 pow (#dimensions)
 * @param characterOffset    the offset to remove from the dimension identifier
 */

case class DimensionalContext(
    dimensionLog: DimensionLog,
    numberOfDimensions: Int,
    dimSpace: Int,
    characterOffset: Int,
    alphabet: Seq[Char])

/**
 * Cube Information
 * @param cube Id of the cube
 * @param maxWeight Maximum weight of the cube
 * @param size Number of elements of the cube
 */

case class CubeInfo(cube: String, maxWeight: Weight, size: Long)

/**
 * Integer values of range [from, to)
 * @param from from value
 * @param to to value
 */
case class RangeValues(from: Weight, to: Weight)

/**
 * Column information.
 * @param column name of the column
 * @param min min value of the column
 * @param max max value of the column
 */
case class ColumnInfo(column: String, min: Double, max: Double)

object ColumnInfo {

  /**
   * Returns the column information for the specified columns of a given data frame.
   *
   * @param dataFrame the data frame
   * @param columns the columns of interest
   * @return the column information
   */
  def get(dataFrame: DataFrame, columns: Seq[String]): Seq[ColumnInfo] = {
    val first :: rest = columns
      .flatMap(column => Seq(min(column), max(column)))
      .map(_.cast(DoubleType))
      .toList
    val aggregates = dataFrame
      .agg(first, rest: _*)
      .first()
    for (i <- columns.indices)
      yield ColumnInfo(
        columns(i),
        aggregates.getAs[Double](2 * i),
        aggregates.getAs[Double](2 * i + 1))
  }

}
