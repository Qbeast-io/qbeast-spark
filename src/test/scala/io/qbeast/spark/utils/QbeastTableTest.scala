package io.qbeast.spark.utils

import io.qbeast.TestClasses.Client3
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.SparkSession

class QbeastTableTest extends QbeastIntegrationTestSpec {

  private def createDF(spark: SparkSession) = {
    val rdd =
      spark.sparkContext.parallelize(
        0.to(1000)
          .map(i => Client3(i * i, s"student-$i", i, (i * 1000 + 123), i * 2567.3432143)))
    spark.createDataFrame(rdd)
  }

  it should "return index metrics" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val data = createDF(spark)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 100

      writeTestData(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      val metrics = qbeastTable.getIndexMetrics()

      metrics.row_count shouldBe data.count()
      metrics.dimensionCount shouldBe columnsToIndex.size
      metrics.nonLeafCubeSizeDetails.min shouldBe <=(metrics.nonLeafCubeSizeDetails.firstQuartile)
      metrics.nonLeafCubeSizeDetails.firstQuartile shouldBe <=(
        metrics.nonLeafCubeSizeDetails.secondQuartile)
      metrics.nonLeafCubeSizeDetails.secondQuartile shouldBe <=(
        metrics.nonLeafCubeSizeDetails.thirdQuartile)
      metrics.nonLeafCubeSizeDetails.thirdQuartile shouldBe <=(metrics.nonLeafCubeSizeDetails.max)

    }
  }

}
