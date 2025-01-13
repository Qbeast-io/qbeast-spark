/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.Point
import io.qbeast.core.model.Revision
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Row

/**
 * Utility functions for working with Spark Rows
 */
object RowUtils {

  /**
   * Converts the row values to a Point in the space
   * @param row
   *   the row values
   * @param revision
   *   the revision of the space
   * @return
   *   the point
   */
  def rowValuesToPoint(row: Row, revision: Revision): Point = Point {
    if (revision.transformations.isEmpty) {
      throw AnalysisExceptionFactory.create("Trying to index on a not initialized Revision")

    }
    val coordinates = Vector.newBuilder[Double]
    coordinates.sizeHint(revision.columnTransformers.length)
    var i = 0
    for (t <- revision.transformations) {
      val v = row.get(i)
      val transformation = t.transform(v)
      assert(
        transformation >= 0.0 && transformation <= 1.0,
        s"Value $v is out of bounds of the Transformation. This can happen if you are indexing a Dataframe that:" +
          s"1. It's source is constantly changing." +
          s"2. The query that produces the DataFrame is not deterministic." +
          s"To avoid this problem, we suggest to: " +
          s"1. Change the transformer type to quantiles." +
          s"2. Add columnStats with a greater space range to avoid indexing errors." +
          s"3. save the DF as delta and Convert it To Qbeast in a second step")
      coordinates += transformation
      i += 1

    }
    coordinates.result()

  }

}
