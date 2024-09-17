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
package io.qbeast.spark.utils

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
      numberOfQuantiles: Int): String = {

    import df.sparkSession.implicits._
    log.info(s"Computing histogram for column $columnName with number of bins $numberOfQuantiles")
    val binStarts = "__bin_starts"
    val stringPartitionColumn =
      MultiDimClusteringFunctions.range_partition_id(col(columnName), numberOfQuantiles)

    val quantiles = df
      .select(columnName)
      .distinct()
      .na
      .drop()
      .groupBy(stringPartitionColumn)
      .agg(min(columnName).alias(binStarts))
      .select(binStarts)
      .orderBy(binStarts)
      .as[String]
      .collect()

    log.info(s"Quantiles for column $columnName: $quantiles")
    quantiles
      .map(string => s"'$string'")
      .mkString("[", ",", "]")
  }

  private def computeQuantilesForNumericColumn(
      df: DataFrame,
      columnName: String,
      numberOfQuantiles: Int): String = {
    log.info(s"Computing quantiles for column $columnName with number of bins $numberOfQuantiles")
    val probabilities = (0 to numberOfQuantiles).map(_ / numberOfQuantiles.toDouble).toArray
    val approxQuantile = df.stat.approxQuantile(columnName, probabilities, 0.1)
    log.info(s"Quantiles for column $columnName: $approxQuantile")
    approxQuantile
      .mkString("[", ",", "]")
  }

  /**
   * Compute the quantiles for a given column
   *
   * Since computing the quantiles can be expensive, this method is used outside the indexing
   * process.
   *
   * It outputs the quantiles of the column as format [bin1, bin2, bin3, ...] Number of bins by
   * default is 50.
   *
   * For example:
   *
   * val qbeastTable = QbeastTable.forPath(spark, "path")
   *
   * val quantiles =qbeastTable.computeQuantilesForColumn(df, "column")
   *
   * df.write.format("qbeast") .option("columnsToIndex","column:quantiles")
   * .option("columnStats",quantiles).save()
   *
   * @param df
   *   DataFrame
   * @param columnName
   *   Column name
   * @param probabilities
   *   Array of probabilities
   * @return
   */
  def computeQuantilesForColumn(
      df: DataFrame,
      columnName: String,
      numberOrQuantiles: Int = 50): String = {
    if (!df.columns.contains(columnName)) {
      throw AnalysisExceptionFactory.create(s"Column $columnName does not exist in the dataframe")
    }
    val dataType = df.schema(columnName).dataType
    dataType match {
      case _ if dataType.isInstanceOf[StringType] =>
        computeQuantilesForStringColumn(df, columnName, numberOrQuantiles)
      case _ if dataType.isInstanceOf[NumericType] =>
        computeQuantilesForNumericColumn(df, columnName, numberOrQuantiles)
      case _ =>
        throw AnalysisExceptionFactory.create(
          s"Column $columnName is of type $dataType. " +
            "Only String and Numeric columns are supported")
    }
  }

}
