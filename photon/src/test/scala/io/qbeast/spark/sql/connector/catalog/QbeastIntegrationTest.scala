package io.qbeast.spark.sql.connector.catalog

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.Sample
import org.apache.spark.sql.execution.{FileSourceScanExec}
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex

class QbeastIntegrationTest extends QbeastTestSpec {

  private def checkFileFiltering(query: DataFrame, path: String): Unit = {
    val leaves = query.queryExecution.executedPlan.collectLeaves()
    val fullIndex =
      new InMemoryFileIndex(query.sparkSession, Seq(new Path(path)), Map.empty, None)
    val allFiles = fullIndex.allFiles()

    leaves
      .foreach {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[InMemoryFileIndex] =>
          val index = f.relation.location.asInstanceOf[InMemoryFileIndex]
          val filesToRead = index.inputFiles
          filesToRead.length shouldBe <(allFiles.length)
      }

  }

  private def checkSamplingPushdown(query: DataFrame): Unit = {
    val analyzed = query.queryExecution.analyzed
    val optimized = query.queryExecution.optimizedPlan

    val sampleOperator = analyzed.collectFirst { case a: Sample => a }
    val sampleOperatorOptimized = optimized.collectFirst { case a: Sample => a }

    sampleOperator shouldBe defined
    sampleOperatorOptimized should not be defined
  }

  "Qbeast Datasource" should "load data in qbeast format" in withSparkAndTmpDir((spark, _) => {

    val rawData = loadRawData(spark)

    val qbeastData = spark.read
      .format("qbeast")
      .load(qbeastDataPath)

    qbeastData.count() shouldBe rawData.count()

    qbeastData.columns.toSet shouldBe rawData.columns.toSet

    assertSmallDatasetEquality(
      qbeastData,
      rawData,
      orderedComparison = false,
      ignoreNullable = true)
  })

  it should "sample accordingly" in withSparkAndTmpDir((spark, _) => {

    val qbeastData = spark.read
      .format("qbeast")
      .load(qbeastDataPath)
    val dataSize = qbeastData.count()

    List(0.1, 0.2, 0.5).foreach(precision => {
      val query = qbeastData
        .sample(withReplacement = false, precision)

      // Check if the Sample operator is being pushed down
      checkSamplingPushdown(query)

      // Check if the files are being filtered by the index
      checkFileFiltering(query, qbeastDataPath)

      val result = query.count()

      // Check if the results contains lesser data than the original size
      // DISCLAIMER: on Sampling we don't filter by record, we only filter the files
      // User can expect size == to dataSize if the percentage to sample is too big
      // Always depending on the index configuration and distribution
      result shouldBe <(dataSize)
    })

  })

  it should "filter pushdown" in withSparkAndTmpDir((spark, _) => {

    val qbeastData = spark.read
      .format("qbeast")
      .load(qbeastDataPath)

    val filterUser = "(user_id < 546280860 and user_id >= 536764969)"

    val query = qbeastData.filter(filterUser)

    // Check if file filtering is used
    checkFileFiltering(query, qbeastDataPath)

    val originalQuery = loadRawData(spark).filter(filterUser)

    // Check if the result is correct
    assertSmallDatasetEquality(
      query,
      originalQuery,
      orderedComparison = false,
      ignoreNullable = true)

  })

}
