/*
 * Copyright 2021 Qbeast Analytics, S.L.
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
