/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{Point, Revision}
import org.apache.spark.sql.{AnalysisExceptionFactory, Column}
import org.apache.spark.sql.functions.{array, col}

object RowUtils {

  def rowValuesToPoint(values: Seq[Any], spaceRevision: Revision): Point = Point {
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
    spaceRevision.transform(coordinates.result())
  }

  def rowValuesColumn(columnsToIndex: Seq[String]): Column =
    array(columnsToIndex.map(col): _*)

}
