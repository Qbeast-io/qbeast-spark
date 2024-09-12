package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.utils.QbeastUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HistogramTransformerIndexingTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec {

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

  it should "create better file-level min-max with a String histogram" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val histPath = tmpDir + "/string_hist/"
      val hashPath = tmpDir + "/string_hash/"
      val colName = "brand"

      val df = loadTestData(spark)

      val colHistStr = QbeastUtils.computeHistogramForColumn(df, colName)
      val statsStr = s"""{"${colName}_histogram":$colHistStr}"""

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("cubeSize", "30000")
        .option("columnsToIndex", s"$colName:histogram")
        .option("columnStats", statsStr)
        .save(histPath)
      val histDist = computeColumnEncodingDist(spark, histPath, colName)

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", colName)
        .option("cubeSize", "30000")
        .save(hashPath)
      val hashDist = computeColumnEncodingDist(spark, hashPath, colName)

      histDist should be < hashDist
    })

  it should "create better file-level min-max with a Int histogram" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val df = 1.to(100).toDF("int_col")
      val colName = "int_col"
      val histPath = tmpDir + "/hist/"
      val hashPath = tmpDir + "/linear/"

      val colHistStr = QbeastUtils.computeHistogramForColumn(df, colName)
      val statsStr = s"""{"${colName}_histogram":$colHistStr}"""

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("cubeSize", "30")
        .option("columnsToIndex", s"$colName:histogram")
        .option("columnStats", statsStr)
        .save(histPath)
      val histDist = computeColumnEncodingDist(spark, histPath, colName)

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", colName)
        .option("cubeSize", "30")
        .save(hashPath)
      val hashDist = computeColumnEncodingDist(spark, hashPath, colName)

      histDist should be < hashDist
    })

}
