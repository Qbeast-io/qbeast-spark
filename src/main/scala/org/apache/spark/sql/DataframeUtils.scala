package org.apache.spark.sql

object DataframeUtils {

  def showString(df: DataFrame, truncate: Int = 40, numRows: Int = 100): String = {
    df.showString(numRows, truncate)
  }

}
