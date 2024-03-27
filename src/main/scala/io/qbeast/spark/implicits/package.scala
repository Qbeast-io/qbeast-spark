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
package io.qbeast.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter

package object implicits {

  /**
   * Extends the DataFrameReader API by adding a qbeast function Usage:
   * {{{
   * spark.read.qbeast(path)
   * }}}
   */
  implicit class QbeastDataFrameReader(private val reader: DataFrameReader) extends AnyVal {

    def qbeast(path: String): DataFrame = {
      reader.format("qbeast").load(path)
    }

  }

  /**
   * Extends the DataFrameWriter API by adding a qbeast function Usage:
   * {{{
   * df.write.qbeast(path)
   * }}}
   */
  implicit class QbeastDataFrameWriter[T](private val dfWriter: DataFrameWriter[T])
      extends AnyVal {

    def qbeast(output: String): Unit = {
      dfWriter.format("qbeast").save(output)
    }

  }

  /**
   * Extends the DataFrame API by adding the Tolerance function
   * {{{
   * df.tolerance(0.1)
   * }}}
   */
  implicit class QbeastDataFrame(val df: DataFrame) {

    def tolerance(precision: Double): DataFrame = {
      // TODO
      ??? // scalastyle:ignore
    }

  }

}
