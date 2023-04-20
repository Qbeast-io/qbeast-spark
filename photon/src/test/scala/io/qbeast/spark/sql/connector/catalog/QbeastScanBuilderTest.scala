package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.datasources.{OTreePhotonIndex, QbeastPhotonSnapshot}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.SparkDataSourceUtils
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class QbeastScanBuilderTest extends QbeastTestSpec {

  behavior of "QbeastScanBuilderTest"

  it should "return pushed filters in pushedFilters" in withSpark { spark =>
    {
      val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
      val optionsScala = Map.empty[String, String]
      val options = new CaseInsensitiveStringMap(optionsScala.asJava)
      val oTreePhotonIndex =
        OTreePhotonIndex(spark, snapshot, optionsScala, None)

      val qbeastScanBuilder =
        new QbeastScanBuilder(spark, snapshot.schema, snapshot.schema, oTreePhotonIndex, options)

      val filterLessThan = (col("user_id") < lit(546280860)).expr
      val filterGreaterThanOrEq = (col("user_id") >= lit(536764969)).expr
      val filters = Seq(filterLessThan, filterGreaterThanOrEq)
      val translatedFilters = filters.map(SparkDataSourceUtils.translateFilter(_, true).get)

      qbeastScanBuilder.pushFilters(filters)
      qbeastScanBuilder.pushedFilters.length shouldBe 2
      qbeastScanBuilder.pushedFilters shouldBe SparkDataSourceUtils.mapFiltersToV2(
        translatedFilters)

    }
  }

  it should "push table sample" in withSpark { spark =>
    {
      val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
      val optionsScala = Map.empty[String, String]
      val options = new CaseInsensitiveStringMap(optionsScala.asJava)
      val oTreePhotonIndex =
        OTreePhotonIndex(spark, snapshot, optionsScala, None)
      val qbeastScanBuilder =
        new QbeastScanBuilder(spark, snapshot.schema, snapshot.schema, oTreePhotonIndex, options)

      val sampleUpperBound = 0.1
      val sampleLowerBound = 0.0
      val withReplacement = false
      val seed = 42

      qbeastScanBuilder.pushTableSample(
        sampleLowerBound,
        sampleUpperBound,
        withReplacement,
        seed) shouldBe true

    }
  }

  it should "throw error if sample range is incorrect" in withSpark { spark =>
    {
      val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
      val optionsScala = Map.empty[String, String]
      val options = new CaseInsensitiveStringMap(optionsScala.asJava)
      val oTreePhotonIndex =
        OTreePhotonIndex(spark, snapshot, optionsScala, None)
      val qbeastScanBuilder =
        new QbeastScanBuilder(spark, snapshot.schema, snapshot.schema, oTreePhotonIndex, options)

      val sampleUpperBound = 0.0
      val sampleLowerBound = 0.1
      val withReplacement = false
      val seed = 42

      a[AnalysisException] shouldBe
        thrownBy(
          qbeastScanBuilder
            .pushTableSample(sampleLowerBound, sampleUpperBound, withReplacement, seed))

    }
  }

  it should "build a ParquetScan" in withSpark { spark =>
    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val optionsScala = Map.empty[String, String]
    val options = new CaseInsensitiveStringMap(optionsScala.asJava)
    val oTreePhotonIndex =
      OTreePhotonIndex(spark, snapshot, optionsScala, None)
    val qbeastScanBuilder =
      new QbeastScanBuilder(spark, snapshot.schema, snapshot.schema, oTreePhotonIndex, options)

    qbeastScanBuilder.build() shouldBe a[ParquetScan]

  }

  it should "filter files when table sample is pushed down" in withSpark { spark =>
    {
      val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
      val optionsScala = Map.empty[String, String]
      val options = new CaseInsensitiveStringMap(optionsScala.asJava)
      val oTreePhotonIndex =
        OTreePhotonIndex(spark, snapshot, optionsScala, None)
      val qbeastScanBuilder =
        new QbeastScanBuilder(spark, snapshot.schema, snapshot.schema, oTreePhotonIndex, options)

      val sampleUpperBound = 0.1
      val sampleLowerBound = 0.0
      val withReplacement = false
      val seed = 42

      qbeastScanBuilder.pushTableSample(sampleLowerBound, sampleUpperBound, withReplacement, seed)
      val parquetScan = qbeastScanBuilder.build().asInstanceOf[ParquetScan]
      val allFilesFiltered = parquetScan.fileIndex.allFiles()
      allFilesFiltered.size shouldBe <(oTreePhotonIndex.allFiles().size)

    }
  }

}
