package io.qbeast.utils

import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.model.StringDataType
import io.qbeast.core.transform.CDFStringQuantilesTransformation
import io.qbeast.core.transform.CDFStringQuantilesTransformer
import io.qbeast.core.transform.IdentityToZeroTransformation
import io.qbeast.core.transform.IdentityTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.core.transform.NullToZeroTransformation
import io.qbeast.core.transform.StringHistogramTransformation
import io.qbeast.core.transform.StringHistogramTransformer
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec

import scala.annotation.nowarn

@nowarn("cat=deprecation")
class TransformationsUtilsTest extends QbeastIntegrationTestSpec {

  "QbeastUtils" should "replace StringHistogramTransformation with CDFQuantiles" in {
    val revisionWithNullToZero = Revision(
      1,
      System.currentTimeMillis(),
      QTableID("abc"),
      10,
      Seq(StringHistogramTransformer("id", StringDataType)).toIndexedSeq,
      Seq(StringHistogramTransformation(IndexedSeq("a", "b"))).toIndexedSeq)

    val revisionUpdated = QbeastUtils.updateTransformationTypes(revisionWithNullToZero)
    revisionUpdated.transformations.head shouldBe a[CDFStringQuantilesTransformation]

  }

  it should "replace NullToZero Transformations" in {
    val revisionWithNullToZero = Revision(
      1,
      System.currentTimeMillis(),
      QTableID("abc"),
      10,
      Seq(LinearTransformer("id", LongDataType)).toIndexedSeq,
      Seq(NullToZeroTransformation).toIndexedSeq)

    val revisionUpdated = QbeastUtils.updateTransformationTypes(revisionWithNullToZero)
    revisionUpdated.transformations.head shouldBe an[IdentityTransformation]

  }

  it should "replace IdentityToZero Transformations" in {
    val revisionWithIdentityToZero = Revision(
      1,
      System.currentTimeMillis(),
      QTableID("abc"),
      10,
      Seq(LinearTransformer("id", LongDataType)).toIndexedSeq,
      Seq(IdentityToZeroTransformation(1)).toIndexedSeq)

    val revisionUpdated = QbeastUtils.updateTransformationTypes(revisionWithIdentityToZero)
    revisionUpdated.transformations.head shouldBe an[IdentityTransformation]

  }

  it should "replace Histogram Transformations with CDFQuantiles" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val histogramTablePath = s"$tmpDir/histogram-table"
      val df = spark.range(5).map(i => s"$i").toDF("id_string")
      df.write
        .format("qbeast")
        .option("columnsToIndex", "id_string:histogram")
        .save(histogramTablePath)

      val histogramTable = QbeastTable.forPath(spark, histogramTablePath)
      val latestRevision = histogramTable.latestRevision
      latestRevision.columnTransformers.head shouldBe a[StringHistogramTransformer]
      latestRevision.transformations.head shouldBe a[StringHistogramTransformation]

      // Update transformation types
      QbeastUtils.updateTransformationTypes(histogramTable)

      histogramTable.latestRevision.columnTransformers.head shouldBe a[
        CDFStringQuantilesTransformer]
      histogramTable.latestRevision.transformations.head shouldBe a[
        CDFStringQuantilesTransformation]

    })

  it should "maintain original transformations if there's no changes" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val tablePath = s"$tmpDir/table"
      val df = spark.range(5).map(i => (s"$i", i)).toDF("id_string", "id")
      df.write
        .format("qbeast")
        .option("columnsToIndex", "id_string,id")
        .save(tablePath)

      val table = QbeastTable.forPath(spark, tablePath)
      val revisionBefore = table.latestRevision

      // Update transformation types
      QbeastUtils.updateTransformationTypes(table)

      val revisionAfter = table.latestRevision
      revisionAfter shouldBe equal(revisionBefore)

    })

}
