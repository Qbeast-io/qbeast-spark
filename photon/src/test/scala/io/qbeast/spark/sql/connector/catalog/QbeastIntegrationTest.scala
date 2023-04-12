package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.datasources.OTreePhotonIndex
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec

class QbeastIntegrationTest extends QbeastTestSpec {

  private def checkFileFiltering(query: DataFrame): Unit = {
    val leaves = query.queryExecution.executedPlan.collectLeaves()

    leaves.exists(p =>
      p
        .asInstanceOf[FileSourceScanExec]
        .relation
        .location
        .isInstanceOf[OTreePhotonIndex]) shouldBe true

    leaves
      .foreach {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[OTreePhotonIndex] =>
          val index = f.relation.location.asInstanceOf[OTreePhotonIndex]
          val matchingFiles =
            index.listFiles(f.partitionFilters, f.dataFilters).flatMap(_.files)
          val allFiles = index.allFiles()
          matchingFiles.length shouldBe <(allFiles.length)
      }

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

      // Check if the files are being filtered by the index
      checkFileFiltering(query)

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
    checkFileFiltering(query)

    val originalQuery = loadRawData(spark).filter(filterUser)

    // Check if the result is correct
    assertSmallDatasetEquality(
      query,
      originalQuery,
      orderedComparison = false,
      ignoreNullable = true)

  })

}
