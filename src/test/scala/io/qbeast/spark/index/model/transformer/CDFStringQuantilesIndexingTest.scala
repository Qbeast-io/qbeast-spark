package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.delta.DefaultFileIndex
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.utils.QbeastUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CDFStringQuantilesIndexingTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec
    with CDFQuantilesTestUtils {

  "CDF Quantile for String values" should "create better file-level min-max with a String quantiles" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val quantilesPath = tmpDir + "/string_quantiles/"
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
        .save(quantilesPath)
      val quantilesDist =
        computeColumnEncodingDistance(spark, quantilesPath, colName, forString = true)

      df.write
        .mode("overwrite")
        .format("qbeast")
        .option("columnsToIndex", colName)
        .option("cubeSize", "30000")
        .save(hashPath)
      val hashDist = computeColumnEncodingDistance(spark, hashPath, colName, forString = true)

      quantilesDist should be < hashDist
    })

  private def getMatchingFilesFromQuery(query: DataFrame): Int = {
    query.queryExecution.executedPlan
      .collectLeaves()
      .filter(_.isInstanceOf[FileSourceScanExec])
      .collectFirst {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[DefaultFileIndex] =>
          val matchingFiles = f.relation.location
            .listFiles(f.partitionFilters, f.dataFilters)
            .flatMap(_.files)
          matchingFiles.length
      }
      .get
  }

  it should "improve file skipping on strings" in withSparkAndTmpDir((spark, tmpDir) => {
    val quantilesPath = tmpDir + "/string_quantiles/"
    val hashPath = tmpDir + "/string_hash/"
    val colName = "brand"
    val cubeSize = 1000

    val df = loadTestData(spark)

    val columnQuantiles = QbeastUtils.computeQuantilesForColumn(df, colName)
    val statsStr = s"""{"${colName}_quantiles":$columnQuantiles}"""

    // QUANTILES
    df.write
      .mode("overwrite")
      .format("qbeast")
      .option("cubeSize", cubeSize)
      .option("columnsToIndex", s"$colName:quantiles")
      .option("columnStats", statsStr)
      .save(quantilesPath)

    // DEFAULT
    df.write
      .mode("overwrite")
      .format("qbeast")
      .option("columnsToIndex", colName)
      .option("cubeSize", cubeSize)
      .save(hashPath)

    // TOTAL FILES
    val totalFilesQuantiles = DefaultFileIndex(spark, new Path(quantilesPath)).inputFiles.length

    // FILTER QUERIES
    val conditionExpr = "brand == 'versace'"
    val quantilesQuery =
      spark.read.format("qbeast").load(quantilesPath).filter(conditionExpr)
    val hashQuery = spark.read.format("qbeast").load(hashPath).filter(conditionExpr)
    val originalQuery = df.filter(conditionExpr)

    // NUMBER FILTERED FILES
    val filesToReadHash = getMatchingFilesFromQuery(hashQuery)
    val filesToReadQuantiles = getMatchingFilesFromQuery(quantilesQuery)

    filesToReadQuantiles shouldBe <(totalFilesQuantiles)
    filesToReadQuantiles shouldBe <(filesToReadHash)

    // ASSERT RESULTS
    assertSmallDatasetEquality(
      quantilesQuery,
      originalQuery,
      orderedComparison = false,
      ignoreNullable = true)
    assertSmallDatasetEquality(
      hashQuery,
      originalQuery,
      orderedComparison = false,
      ignoreNullable = true)

  })

}
