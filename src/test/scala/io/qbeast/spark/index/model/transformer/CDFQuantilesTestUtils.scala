package io.qbeast.spark.index.model.transformer

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

trait CDFQuantilesTestUtils {

  /**
   * Compute weighted encoding distance for files: (ascii(string_col_max.head) -
   * ascii(string_col_min.head)) * numRecords
   */
  def computeColumnEncodingDist(
      spark: SparkSession,
      tablePath: String,
      columnName: String): Long = {
    import spark.implicits._

    val dl = DeltaLog.forTable(spark, tablePath)
    val js = dl
      .update()
      .allFiles
      .select("stats")
      .collect()
      .map(r => r.getAs[String](0))
      .mkString("[", ",", "]")
    val stats = spark.read.json(Seq(js).toDS())

    stats
      .select(
        col(s"maxValues.$columnName").alias("__max"),
        col(s"minValues.$columnName").alias("__min"),
        col("numRecords"))
      .withColumn("__max_start", substring(col("__max"), 0, 1))
      .withColumn("__min_start", substring(col("__min"), 0, 1))
      .withColumn("__max_ascii", ascii(col("__max_start")))
      .withColumn("__min_ascii", ascii(col("__min_start")))
      .withColumn("dist", abs(col("__max_ascii") - col("__min_ascii")) * col("numRecords"))
      .select("dist")
      .agg(sum("dist"))
      .first()
      .getAs[Long](0)
  }

}
