package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.utils.QbeastUtils
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType

class NumericQuantileTest extends QbeastIntegrationTestSpec {

  "Indexing with the Approximate Percentale" should "work" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._

      val qbeastDefault = tmpDir + "/default"
      val qbeastWithPercentile = tmpDir + "/percentile"
      val df = spark.range(0, 1000).map(i => (i, i.toString, i * 2L)).toDF("a", "b", "c")
      val columnName = "a"

      // Write the data
      df.write.format("qbeast").option("columnsToIndex", s"$columnName").save(qbeastDefault)

      // calculate the percentiles
      val quantileRanges = (1 to 10).map(_ / 10.0).toArray
      val relativeError = 0.1
      val approxQuantiles =
        df.stat.approxQuantile(columnName, quantileRanges, relativeError).toIndexedSeq
      println(s"APPROX QUANTILES FOR COLUMN $columnName")
      approxQuantiles.foreach(println)

      val quantileUDF = udf((value: Double) => {
        val resultIndex = approxQuantiles.zipWithIndex.minBy { case (v, _) =>
          math.abs(v - value)
        }._2 / (approxQuantiles.size)
      })

      spark.udf.register("quantile_value", quantileUDF.asNondeterministic())
      val dfWithQuantiles = df
        .withColumn("a_quantile", quantileUDF(col("a").cast(DoubleType)))
      dfWithQuantiles.show()

      dfWithQuantiles.write
        .format("qbeast")
        .option("columnsToIndex", "a_quantile")
        .save(qbeastWithPercentile)

      val indexMetricsDefault = QbeastTable.forPath(spark, qbeastDefault).getIndexMetrics()
      println("INDEX METRICS DEFAULT")
      println(indexMetricsDefault.toString)
      val indexMetricsWithPercentile =
        QbeastTable.forPath(spark, qbeastWithPercentile).getIndexMetrics()
      println("INDEX METRICS PERCENTILE")
      println(indexMetricsWithPercentile)
    })

  it should "work with input columnStats" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {

    val qbeastDefault = tmpDir + "/default"
    val qbeastWithQuantiles = tmpDir + "/quantile"
    val df = loadTestData(spark)
    val columnName = "user_id"

    // SAVE DEFAULT
    df.write.format("qbeast").option("columnsToIndex", s"$columnName").save(qbeastDefault)

    val quantileRanges = (0 to 10).map(_ / 10.0).toArray
    val relativeError = 0.1
    val approxQuantiles =
      df.stat.approxQuantile(columnName, quantileRanges, relativeError)
    println(s"APPROX QUANTILES FOR COLUMN $columnName")
    approxQuantiles.foreach(println)

    val columnQuantilesString = QbeastUtils.computeQuantilesForColumn(df, "a", 10)
    val statsStr = s"""{"${columnName}_quantiles":$columnQuantilesString}"""

    df.write
      .mode("overwrite")
      .format("qbeast")
      .option("cubeSize", "30000")
      .option("columnsToIndex", s"$columnName:quantiles")
      .option("columnStats", statsStr)
      .save(qbeastWithQuantiles)

    val indexMetricsDefault = QbeastTable.forPath(spark, qbeastDefault).getIndexMetrics()
    println("INDEX METRICS DEFAULT")
    println(indexMetricsDefault.toString)
    val indexMetricsWithQuantile =
      QbeastTable.forPath(spark, qbeastWithQuantiles).getIndexMetrics()
    println("INDEX METRICS PERCENTILE")
    println(indexMetricsWithQuantile)
  })

}
