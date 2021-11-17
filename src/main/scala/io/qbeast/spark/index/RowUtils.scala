/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{Point, Revision}
import org.apache.spark.sql.{AnalysisExceptionFactory, Column}
import org.apache.spark.sql.functions.{array, col}

/**
 * Utility functions for working with Spark Rows
 */
object RowUtils {

  /**
   * Converts the row values to a Point in the space
   * @param values the values of the columns
   * @param revision the revision of the space
   * @return the point
   */
  def rowValuesToPoint(values: Seq[Any], revision: Revision): Point = Point {
    val coordinates = Vector.newBuilder[Double]
    coordinates.sizeHint(values.length)
    for (value <- values) {
      value match {
        case n: Number =>
          coordinates += n.doubleValue()
        case null =>
          throw AnalysisExceptionFactory.create(
            "Column to index contains null values. Please initialize them before indexing")
        case _ =>
          throw AnalysisExceptionFactory.create("Column to index contains non-numeric value")
      }
    }
    revision.transform(coordinates.result())
  }

  /**
   * Converts the column names into Spark Columns
   * @param columnNames the column names
   * @return
   */
  def rowValuesColumn(columnNames: Seq[String]): Column =
    array(columnNames.map(col): _*)

}
