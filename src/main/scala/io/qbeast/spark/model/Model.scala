/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

import io.qbeast.spark.index.Weight
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.DoubleType

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
