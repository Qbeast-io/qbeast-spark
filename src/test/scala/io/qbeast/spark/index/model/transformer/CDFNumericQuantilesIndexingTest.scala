package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.utils.QbeastUtils
import io.qbeast.spark.QbeastIntegrationTestSpec

class CDFNumericQuantilesIndexingTest
    extends QbeastIntegrationTestSpec
    with CDFQuantilesTestUtils {

  "CDF Quantiles for numeric columns" should "create better file-level min-max than Linear" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val df = 1.to(100).toDF("int_col")
      val colName = "int_col"
      val quantilesPath = tmpDir + "/quantiles/"
      val hashPath = tmpDir + "/linear/"

      val colQuantilesDist = QbeastUtils.computeQuantilesForColumn(df, colName)
      val statsStr = s"""{"${colName}_quantiles":$colQuantilesDist}"""

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("cubeSize", "30")
        .option("columnsToIndex", s"$colName:quantiles")
        .option("columnStats", statsStr)
        .save(quantilesPath)
      val quantilesDist = computeColumnEncodingDist(spark, quantilesPath, colName)

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", colName)
        .option("cubeSize", "30")
        .save(hashPath)
      val hashDist = computeColumnEncodingDist(spark, hashPath, colName)

      quantilesDist should be < hashDist
    })

  it should "allow append without specifiying columnStats" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val df = 1.to(10).toDF("int_col")
      val colName = "int_col"
      val path = tmpDir + "/quantiles/"
      val statsStr = s"""{"${colName}_quantiles":[0.1, 0.4, 0.7, 0.9]}"""

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("cubeSize", "30")
        .option("columnsToIndex", s"$colName:quantiles")
        .option("columnStats", statsStr)
        .save(path)

      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", colName)
        .option("cubeSize", "30")
        .save(path)

      val indexed = spark.read.format("qbeast").load(path)
      indexed.count() shouldBe 20
      assertSmallDatasetEquality(df.union(df), indexed, ignoreNullable = true, orderedComparison = false)
    })

  it should "trigger new revision when columnStats changes"

  it should "throw error when no columnStats are provided and column is not indexed"

}
