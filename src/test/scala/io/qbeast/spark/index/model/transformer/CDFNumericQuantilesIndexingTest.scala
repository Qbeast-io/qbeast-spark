package io.qbeast.spark.index.model.transformer

import io.qbeast.core.transform.CDFNumericQuantilesTransformation
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable

class CDFNumericQuantilesIndexingTest
    extends QbeastIntegrationTestSpec
    with CDFQuantilesTestUtils {

  "Quantiles index" should "allow append without specifiying columnStats" in withSparkAndTmpDir(
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
      assertSmallDatasetEquality(
        df.union(df),
        indexed,
        ignoreNullable = true,
        orderedComparison = false)
    })

  it should "trigger new revision when columnStats changes" in withSparkAndTmpDir(
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

      val newStatsStr = s"""{"${colName}_quantiles":[0.1, 0.4, 0.7, 0.8, 0.9]}"""
      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", s"$colName:quantiles")
        .option("columnStats", newStatsStr)
        .save(path)

      // Get the QbeastTable
      val qbeastTable = QbeastTable.forPath(spark, path)
      val allRevisions = qbeastTable.allRevisions()
      allRevisions.foreach(println)
      allRevisions.size shouldBe 3

      // Check that the first and last revisions have the correct transformations

      // The first revision should have the original quantiles
      val firstRevision = allRevisions(1)
      val firstRevisionTransformations = firstRevision.transformations
      firstRevisionTransformations.size shouldBe 1
      firstRevisionTransformations.head shouldBe a[CDFNumericQuantilesTransformation]
      firstRevisionTransformations.head
        .asInstanceOf[CDFNumericQuantilesTransformation]
        .quantiles shouldBe Seq(0.1, 0.4, 0.7, 0.9)

      // The last revision should have the new quantiles
      val latestRevision = allRevisions(2) // latest revision
      val latestRevisionTransformations = latestRevision.transformations
      latestRevisionTransformations.size shouldBe 1
      latestRevisionTransformations.head shouldBe a[CDFNumericQuantilesTransformation]
      latestRevisionTransformations.head
        .asInstanceOf[CDFNumericQuantilesTransformation]
        .quantiles shouldBe Seq(0.1, 0.4, 0.7, 0.8, 0.9)
    })

  it should "throw Unsupported Operation Exception" +
    " when no columnStats are provided and column is not indexed" in withSparkAndTmpDir(
      (spark, tmpDir) => {

        import spark.implicits._
        val df = 1.to(10).toDF("int_col")
        val colName = "int_col"
        val path = tmpDir + "/quantiles/"

        val thrown = intercept[UnsupportedOperationException] {
          df.write
            .mode("append")
            .format("qbeast")
            .option("columnsToIndex", s"$colName:quantiles")
            .option("cubeSize", "30")
            .save(path)
        }
        thrown.getMessage should include("ManualPlaceholderTransformation does not support")
      })

}
