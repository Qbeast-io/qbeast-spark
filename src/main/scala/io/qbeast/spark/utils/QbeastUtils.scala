package io.qbeast.spark.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.skipping.MultiDimClusteringFunctions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.log
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame

/**
 * Utility object for indexing methods outside the box
 */
object QbeastUtils extends Logging {

  /**
   * Compute the histogram for a given column
   *
   * Since computing the histogram can be expensive, this method is used outside the indexing
   * process.
   *
   * It outputs the histogram of the column as format [bin1, bin2, bin3, ...] Number of bins by
   * default is 50
   *
   * For example:
   *
   * val qbeastTable = QbeastTable.forPath(spark, "path")
   *
   * val histogram =qbeastTable.computeHistogramForColumn(df, "column")
   *
   * df.write.format("qbeast").option("columnsToIndex",
   * "column:histogram").option("columnStats",histogram).save()
   *
   * @param df
   * @param columnName
   */
  def computeHistogramForColumn(df: DataFrame, columnName: String, numBins: Int = 50): String = {

    import df.sparkSession.implicits._
    if (!df.columns.contains(columnName)) {
      throw AnalysisExceptionFactory.create(s"Column $columnName does not exist in the dataframe")
    }

    log.info(s"Computing histogram for column $columnName with number of bins $numBins")
    val binStarts = "__bin_starts"
    val stringPartitionColumn =
      MultiDimClusteringFunctions.range_partition_id(col(columnName), numBins)

    val histogram = df
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

    log.info(s"Histogram for column $columnName: $histogram")
    histogram
      .map(string => s"'$string'")
      .mkString("[", ",", "]")
  }

}
