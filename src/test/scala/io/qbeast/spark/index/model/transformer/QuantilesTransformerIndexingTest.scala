package io.qbeast.spark.index.model.transformer

import io.qbeast.core.transform.QuantilesTransformation
import io.qbeast.spark.utils.QbeastUtils
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable

class QuantilesTransformerIndexingTest extends QbeastIntegrationTestSpec {

  "QuantilesTrasfomer" should "initialize the transformation with input columnStats" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

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

      val columnQuantilesString =
        QbeastUtils.computeQuantilesForColumn(df, columnName, quantileRanges)

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("cubeSize", "30000")
        .option("columnsToIndex", s"$columnName:quantiles")
        .option("columnStats", s"""{"${columnName}_quantiles":$columnQuantilesString}""")
        .save(qbeastWithQuantiles)

      val qbeastTable = QbeastTable.forPath(spark, qbeastWithQuantiles)
      val transformation = qbeastTable.latestRevision.transformations.head
      transformation.isInstanceOf[QuantilesTransformation] shouldBe true
      transformation.asInstanceOf[QuantilesTransformation].quantiles should be(approxQuantiles)

      val indexMetricsDefault = QbeastTable.forPath(spark, qbeastDefault).getIndexMetrics()
      println("INDEX METRICS DEFAULT")
      println(indexMetricsDefault.toString)
      val indexMetricsWithQuantile =
        QbeastTable.forPath(spark, qbeastWithQuantiles).getIndexMetrics()
      println("INDEX METRICS PERCENTILE")
      println(indexMetricsWithQuantile)
    })

}
