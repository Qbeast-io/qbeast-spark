/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{Point, Revision}
import org.apache.spark.sql.{AnalysisExceptionFactory, Row}

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
  def rowValuesToPoint(values: Row, revision: Revision): Point = Point {
    if (revision.transformations.isEmpty) {
      throw AnalysisExceptionFactory.create("Trying to index on a not initialized Revision")

    }
    val coordinates = Vector.newBuilder[Double]
    coordinates.sizeHint(revision.columnTransformers.length)
    var i = 0
    for (t <- revision.transformations) {
      val v = values.get(i)
      i += 1
      if (v == null) {
        throw AnalysisExceptionFactory.create(
          "Column to index contains null values. Please initialize them before indexing")

      }
      coordinates += t.transform(v)

    }
    coordinates.result()

  }

}
