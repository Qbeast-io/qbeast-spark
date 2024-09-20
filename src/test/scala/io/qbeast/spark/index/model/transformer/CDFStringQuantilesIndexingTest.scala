package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.utils.QbeastUtils
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFStringQuantilesIndexingTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec
    with CDFQuantilesTestUtils {

  "CDF Quantile for String values" should "create better file-level min-max with a String quantiles" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val histPath = tmpDir + "/string_quantiles/"
      val hashPath = tmpDir + "/string_hash/"
      val colName = "brand"

      val df = loadTestData(spark)

      val columnQuantiles = QbeastUtils.computeQuantilesForColumn(df, colName)
      val statsStr = s"""{"${colName}_quantiles":$columnQuantiles}"""

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("cubeSize", "30000")
        .option("columnsToIndex", s"$colName:quantiles")
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

}
