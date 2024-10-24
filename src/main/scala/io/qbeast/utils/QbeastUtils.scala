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
package io.qbeast.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.skipping.MultiDimClusteringFunctions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame

/**
 * Utility object for indexing methods outside the box
 */
object QbeastUtils extends Logging {

  /**
   * Compute the quantiles for a given column of type String
   *
   * @param df
   *   the DataFrame
   * @param columnName
   *   the name of the column
   */
  private def computeQuantilesForStringColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int): Array[String] = {

    import df.sparkSession.implicits._
    val binStarts = "__bin_starts"
    val stringPartitionColumn =
      MultiDimClusteringFunctions.range_partition_id(col(columnName), numberOfQuantiles)

    val quantiles = df
      .select(columnName)
      .na
      .drop()
      .groupBy(stringPartitionColumn)
      .agg(min(columnName).alias(binStarts))
      .select(binStarts)
      .orderBy(binStarts)
      .as[String]
      .collect()

    log.info(s"String Quantiles for column $columnName: $quantiles")
    quantiles.map(string => s"'$string'")

  }

  private def computeQuantilesForNumericColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int,
      relativeError: Double): Array[Double] = {
    val probabilities = (0 to numberOfQuantiles).map(_ / numberOfQuantiles.toDouble).toArray
    val reducedDF = df.select(columnName)
    val approxQuantile = reducedDF.stat.approxQuantile(columnName, probabilities, relativeError)
    log.info(s"Numeric Quantiles for column $columnName: ${approxQuantile.mkString(",")}")
    approxQuantile
  }

  /**
   * Compute a sequence of numberOfQuantiles (default 50) within a relativeError (default 0.1) for
   * a given column
   *
   * Since computing the quantiles can be expensive, this method is used outside the indexing
   * process.
   *
   * It outputs the quantiles of the column as an Array[value1, value2, value2, ...] of size equal
   * to the numberOfQuantiles parameter.
   *
   * For example:
   *
   * val qbeastTable = QbeastTable.forPath(spark, "path")
   *
   * val quantiles = qbeastTable.computeQuantilesForColumn(df, "column")
   *
   * df.write.format("qbeast") .option("columnsToIndex","column:quantiles")
   * .option("columnStats",s"""{"column_quantiles":quantiles}""").save()
   *
   * @param df
   *   DataFrame
   * @param columnName
   *   Column name
   * @param numberOfQuantiles
   *   Number of Quantiles, default is 50
   * @param relativeError
   *   Relative Error, default is 0.1
   * @return
   */
  def computeQuantilesForColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int = 50,
      relativeError: Double = 0.1): String = {
    require(numberOfQuantiles > 1, "Number of quantiles must be greater than 1")
    // Check if the column exists
    if (!df.columns.contains(columnName)) {
      throw AnalysisExceptionFactory.create(s"Column $columnName does not exist in the dataframe")
    }
    val dataType = df.schema(columnName).dataType

    log.info(s"Computing quantiles for column $columnName with number of bins $numberOfQuantiles")
    // Compute the quantiles based on the data type
    val quantilesArray: Array[_] = dataType match {
      case StringType =>
        computeQuantilesForStringColumn(df, columnName, numberOfQuantiles)
      case _: NumericType =>
        computeQuantilesForNumericColumn(df, columnName, numberOfQuantiles, relativeError)
      case _ =>
        throw AnalysisExceptionFactory.create(
          s"Column $columnName is of type $dataType. " +
            "Only StringType and NumericType columns are supported.")
    }
    // Return the quantiles as a string
    quantilesArray.mkString("[", ", ", "]")
  }

}
