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
package io.qbeast.spark.index.model.transformer

import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.QTableID
import io.qbeast.core.transform.IdentityTransformation
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformerIndexingTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  // Write source data indexing all columns and read it back
  private def writeAndReadDF(source: Dataset[_], tmpDir: String, spark: SparkSession) = {
    source.write
      .format("qbeast")
      .option("columnsToIndex", source.columns.mkString(","))
      .option("cubeSize", 10000)
      .save(tmpDir)

    spark.read
      .format("qbeast")
      .load(tmpDir)
  }

  "Qbeast spark" should "index tables with string" in withSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => T1(i, s"$i", i.toDouble))
      .as[T1]

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,b,c")
      .option("cubeSize", 10000)
      .save(tmpDir)

    val indexed = spark.read
      .format("qbeast")
      .load(tmpDir)
      .as[T1]

    source.count() shouldBe indexed.count()

    assertSmallDatasetEquality[T1](
      source,
      indexed,
      ignoreNullable = true,
      orderedComparison = false)

  })

  it should
    "index tables with hashing configuration" in withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val source = spark
        .range(100001)
        .map(i => T2(i, i.toDouble))

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a:hashing,c:hashing")
        .option("cubeSize", 10000)
        .save(tmpDir)

      val indexed = spark.read
        .format("qbeast")
        .load(tmpDir)
        .as[T2]

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with all String" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestStrings(s"${i * 2}", s"$i", s"$i$i"))
      .as[TestStrings]
    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestStrings]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with non-numeric string" in withSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestStrings(s"some_string$i", s"some_other_string$i", i.toString))
      .as[TestStrings]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestStrings]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Double" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestDouble((i * i).toDouble, i.toDouble, (i * 2).toDouble))
      .as[TestDouble]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestDouble]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Int" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(_.toInt)
      .map(i => TestInt(i * i, i, i * 2))

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestInt]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with BigDecimal" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestBigDecimal(BigDecimal(i * i), BigDecimal(i), BigDecimal(i * 2)))
      .as[TestBigDecimal]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestBigDecimal]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Float" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestFloat(i * i, i.toInt, i * 2))
      .as[TestFloat]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestFloat]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Long" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestLong(i * i, i, i * 2))
      .as[TestLong]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestLong]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Timestamps" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df =
      Seq(
        "2017-01-01 12:02:00",
        "2017-01-02 12:02:00",
        "2017-01-03 12:02:00",
        "2017-01-04 12:02:00").toDF("date")
    val source = df.withColumn("my_date", to_timestamp($"date"))

    val indexed = writeAndReadDF(source, tmpDir, spark)

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Dates" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df =
      Seq("2017-01-01", "2017-01-02", "2017-01-03", "2017-01-04").toDF("date")
    val source = df.withColumn("my_date", to_date($"date"))

    val indexed = writeAndReadDF(source, tmpDir, spark)

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with multiple rows of a unique Timestamp" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq(
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00").toDF("date")
      val source = df.withColumn("my_date", to_timestamp($"date"))

      val indexed = writeAndReadDF(source, tmpDir, spark)

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with multiple rows of a unique Date" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq("2017-01-01", "2017-01-01", "2017-01-01", "2017-01-01").toDF("date")
      val source = df.withColumn("my_date", to_date($"date"))

      val indexed = writeAndReadDF(source, tmpDir, spark)

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with null values" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(_.toInt)
      .map(i =>
        if (i % 2 == 0) TestNull(Some(s"student$i"), None, Some(i * 2))
        else TestNull(Some(s"student$i"), Some(i), Some(i * 2)))
      .as[TestNull]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestNull]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, orderedComparison = false)

  })

  it should "index tables with ALL null values" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestNull(Some(s"student$i"), None, Some(i * 2)))
      .as[TestNull]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestNull]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, orderedComparison = false)

  })

  it should "index tables with the same value in all rows" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val source = spark
        .range(100000)
        .map(i => TestNull(Some(s"student$i"), Some(10), Some(i)))

      val indexed = writeAndReadDF(source, tmpDir, spark).as[TestNull]

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(source, indexed, orderedComparison = false)
    })

  it should "don't miss records when indexing null string" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      // Reproducing a particular GitHub Archive dataset
      // with all null values in one column
      // and poor cardinality (4 groups) in the other
      import spark.implicits._
      val source = spark
        .range(200000)
        .map(i => TestNull(None, None, Some(i % 4)))

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", 10000)
        .save(tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)

      val is_null = """a is null"""
      indexed.where(is_null).count() shouldBe 200000

      (1 to 4).foreach(i => {
        val filter = s"""a is null and c == $i"""
        indexed.where(filter).count() shouldBe source.where(filter).count()
      })

    })

  it should
    "be able to handle an alternating identity append sequence: " +
    "[null] + [identity_1] + [null] + [identity_2]" in withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val nullDf = spark.range(100).map(_ => TestNull(None, None, None))
      val identityDf_1 = spark.range(100).map(_ => TestNull(None, None, Some(1L)))
      val identityDf_2 = spark.range(100).map(_ => TestNull(None, None, Some(10L)))
      // IdentityTransformation(null, _)
      nullDf.write
        .mode("overwrite")
        .option("columnsToIndex", "c")
        .option("cubeSize", "1000")
        .format("qbeast")
        .save(tmpDir)

      // IdentityTransformation(1, _)
      identityDf_1.write.mode("append").format("qbeast").save(tmpDir)

      // IdentityTransformation(1, _)
      nullDf.write.mode("append").format("qbeast").save(tmpDir)

      // LinearTransformation(1, 10, _, _)
      identityDf_2.write.mode("append").format("qbeast").save(tmpDir)

      val snapshot = DeltaQbeastSnapshot(QTableID(tmpDir))
      val Seq(_, idNullRev, idOneRev, linearRev) = snapshot.loadAllRevisions.sortBy(_.revisionID)
      idNullRev.columnTransformers.head should matchPattern { case _: LinearTransformer => }
      idNullRev.transformations.head should matchPattern {
        case IdentityTransformation(null, LongDataType) =>
      }

      idOneRev.columnTransformers.head should matchPattern { case _: LinearTransformer => }
      idOneRev.transformations.head should matchPattern {
        case IdentityTransformation(1L, LongDataType) =>
      }

      linearRev.columnTransformers.head should matchPattern { case _: LinearTransformer => }
      linearRev.transformations.head should matchPattern {
        case LinearTransformation(1L, 10L, _, LongDataType) =>
      }
    })

  it should "be able to handle identity + regular appends" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._

      val identityDf = spark.range(100).map(_ => TestNull(None, None, Some(-1L)))
      identityDf.write
        .mode("overwrite")
        .option("columnsToIndex", "c")
        .option("cubeSize", "1000")
        .format("qbeast")
        .save(tmpDir)

      val regularDf = spark.range(100).map(i => TestNull(None, None, Some(i.toLong)))
      regularDf.write.mode("append").format("qbeast").save(tmpDir)

      val revision = DeltaQbeastSnapshot(QTableID(tmpDir)).loadLatestRevision
      revision.columnTransformers.head should matchPattern { case _: LinearTransformer => }
      revision.transformations.head should matchPattern {
        case LinearTransformation(-1L, 99L, _, LongDataType) =>
      }
    })

  it should "be able to read depecrated StringHistogramTransfromation" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val histogramTablePath = "src/test/resources/string-histogram-table"
      val histogramTable = QbeastTable.forPath(spark, histogramTablePath)
      val latestRevision = histogramTable.latestRevision
      println(latestRevision.columnTransformers.head.toString)
      println(latestRevision.transformations.head)
      val histogramTableDF = spark.read
        .format("qbeast")
        .load(histogramTablePath)
      val deltaTableDF = spark.read
        .format("delta")
        .load(histogramTablePath)
      histogramTableDF.count() shouldBe deltaTableDF.count()

      val filteredBrandQbeast = histogramTableDF.filter("brand = 'versace'")
      val filteredBrandDelta = deltaTableDF.filter("brand = 'versace'")

      filteredBrandQbeast.count() shouldBe filteredBrandDelta.count()
    })

  it should "throw an exception when using 'histogram' transformer type" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df = spark.range(5).map(i => s"$i").toDF("id_string")
      an[AnalysisException] should be thrownBy {
        df.write
          .format("qbeast")
          .option("columnsToIndex", "id_string:histogram")
          .save(tmpDir)
      }

    })

}
