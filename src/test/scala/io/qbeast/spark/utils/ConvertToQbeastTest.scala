/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.utils

import io.qbeast.core.model.StagingUtils
import io.qbeast.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.utils.QbeastExceptionMessages.incorrectIdentifierFormat
import io.qbeast.spark.utils.QbeastExceptionMessages.partitionedTableExceptionMsg
import io.qbeast.spark.utils.QbeastExceptionMessages.unsupportedFormatExceptionMsg
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.scalatest.PrivateMethodTester

class ConvertToQbeastTest
    extends QbeastIntegrationTestSpec
    with PrivateMethodTester
    with StagingUtils {
  val dataSize = 50000
  val numSparkPartitions = 20

  val columnsToIndex: Seq[String] = Seq("user_id", "price", "event_type")
  val dcs = 5000

  val partitionedParquetExceptionMsg: String =
    partitionedTableExceptionMsg + "Failed to convert the parquet table into delta: "

  def convertFromFormat(
      spark: SparkSession,
      format: String,
      tablePath: String,
      isPartitioned: Boolean = false): Unit = {
    val data = loadTestData(spark).limit(dataSize).repartition(numSparkPartitions)

    // Write source data
    if (isPartitioned) {
      data.write
        .mode("overwrite")
        .partitionBy("event_type")
        .format(format)
        .save(tablePath)
    } else {
      data.write.mode("overwrite").format(format).save(tablePath)
    }

    // Convert source data to qbeast
    val tableIdentifier = s"$format.`$tablePath`"
    ConvertToQbeastCommand(tableIdentifier, columnsToIndex, dcs).run(spark)
  }

  behavior of "ConvertToQbeastCommand"

  it should "convert a delta table" in withSparkAndTmpDir((spark, tmpDir) => {
    val fileFormat = "delta"
    convertFromFormat(spark, fileFormat, tmpDir)

    val sourceDf = spark.read.format(fileFormat).load(tmpDir)
    val qbeastDf = spark.read.format("qbeast").load(tmpDir)

    assertLargeDatasetEquality(qbeastDf, sourceDf, orderedComparison = false)

    // All non-qbeast files are considered staging files and are placed
    // directly into the staging revision(RevisionID = 0)
    val indexStatus = getQbeastSnapshot(tmpDir).loadIndexStatus(stagingID)
    indexStatus.cubesStatuses.size shouldBe 1

    val valuesToTransform = Vector(544496263, 76.96, "view")
    indexStatus.revision.transform(valuesToTransform) shouldBe Vector(0d, 0d, 0d)

  })

  it should "convert a parquet table" in withSparkAndTmpDir((spark, tmpDir) => {
    val fileFormat = "parquet"
    convertFromFormat(spark, fileFormat, tmpDir)

    val sourceDf = spark.read.format(fileFormat).load(tmpDir)
    val qbeastDf = spark.read.format("qbeast").load(tmpDir)

    assertLargeDatasetEquality(qbeastDf, sourceDf, orderedComparison = false)

    // All non-qbeast files are considered staging files and are placed
    // directly into the staging revision(RevisionID = 0)
    val indexStatus = getQbeastSnapshot(tmpDir).loadIndexStatus(stagingID)
    indexStatus.cubesStatuses.size shouldBe 1
  })

  it should "fail to convert a PARTITIONED delta table" in withSparkAndTmpDir((spark, tmpDir) => {
    val fileFormat = "delta"

    val thrown =
      the[AnalysisException] thrownBy
        convertFromFormat(spark, fileFormat, tmpDir, isPartitioned = true)

    thrown.getMessage() should startWith(partitionedTableExceptionMsg)
  })

  it should "fail to convert a PARTITIONED parquet table" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val fileFormat = "parquet"

      val thrown =
        the[AnalysisException] thrownBy
          convertFromFormat(spark, fileFormat, tmpDir, isPartitioned = true)

      thrown.getMessage() should startWith(partitionedParquetExceptionMsg)
    })

  it should "fail to convert an unsupported format" in withSparkAndTmpDir((spark, tmpDir) => {
    val fileFormat = "json"

    val thrown =
      the[AnalysisException] thrownBy convertFromFormat(spark, fileFormat, tmpDir)

    // json not supported
    thrown.getMessage() should startWith(unsupportedFormatExceptionMsg("json"))
  })

  it should "not create new revisions for a qbeast table" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      loadTestData(spark)
        .limit(dataSize)
        .write
        .format("qbeast")
        .option("columnsToIndex", columnsToIndex.mkString(","))
        .option("cubeSize", dcs)
        .save(tmpDir)

      val revisionsBefore = getQbeastSnapshot(tmpDir).loadAllRevisions
      ConvertToQbeastCommand(s"qbeast.`$tmpDir`", columnsToIndex, dcs).run(spark)
      val revisionsAfter = getQbeastSnapshot(tmpDir).loadAllRevisions

      // Revisions should not modify
      revisionsAfter shouldBe revisionsBefore
    })

  it should "fail to convert when the identifier format is not correct" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val identifier = s"parquet`$tmpDir`"
      val thrown = the[AnalysisException] thrownBy
        ConvertToQbeastCommand(identifier, columnsToIndex, dcs).run(spark)

      thrown.getMessage shouldBe incorrectIdentifierFormat(identifier)
    })

  it should "convert if the path contains '.'" in withSparkAndTmpDir((spark, tmpDir) => {
    val location = s"$tmpDir/test.db/table"
    val identifier = s"parquet.`$location`"
    loadTestData(spark)
      .limit(dataSize)
      .write
      .format("delta")
      .save(location)
    ConvertToQbeastCommand(identifier, columnsToIndex, dcs).run(spark)
    getQbeastSnapshot(location).loadAllRevisions.size shouldBe 1
  })

  it should "preserve sampling accuracy" in withSparkAndTmpDir((spark, tmpDir) => {
    convertFromFormat(spark, "parquet", tmpDir)

    val convertedTable = spark.read.format("qbeast").load(tmpDir)
    val tolerance = 0.01

    List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(f => {
      val sampleSize = convertedTable
        .sample(withReplacement = false, f)
        .count()
        .toDouble

      val margin = dataSize * f * tolerance
      sampleSize shouldBe (dataSize * f) +- margin
    })
  })

  "Appending to a converted table" should "create a new, non-staging revision" in
    withSparkAndTmpDir((spark, tmpDir) => {
      convertFromFormat(spark, "parquet", tmpDir)

      // Append qbeast data
      loadTestData(spark)
        .limit(dataSize)
        .write
        .mode("append")
        .format("qbeast")
        .save(tmpDir)

      // Should add new revision
      val qs = getQbeastSnapshot(tmpDir)
      val allRevisions = qs.loadAllRevisions
      val rev = qs.loadLatestRevision

      allRevisions.size shouldBe 2
      isStaging(rev) shouldBe false
    })

}
