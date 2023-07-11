package io.qbeast.spark.utils

import io.qbeast.core.model.CubeId
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.SparkSession
import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID

class QbeastOptimizeIntegrationTest extends QbeastIntegrationTestSpec {

  def optimize(spark: SparkSession, tmpDir: String, times: Int): Unit = {
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)
    (0 until times).foreach(_ => qbeastTable.optimize())
  }

  "the Qbeast data source" should
    "not replicate any point if there are optimizations" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {
          val data = loadTestData(spark)
          writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

          val indexed = spark.read.format("parquet").load(tmpDir)
          assertLargeDatasetEquality(indexed, data, orderedComparison = false)

        }
    }

  "An optimized index" should "sample correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val df = spark.read.format("qbeast").load(tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)
        val dataSize = data.count()

        df.count() shouldBe dataSize

        val tolerance = 0.01
        List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(precision => {
          val result = df
            .sample(withReplacement = false, precision)
            .count()
            .toDouble

          result shouldBe (dataSize * precision) +- dataSize * precision * tolerance
        })
      }
  }

  it should "erase cube information when overwritten" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        // val tmpDir = "/tmp/qbeast3"
        val data = loadTestData(spark)

        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        // analyze and optimize the index 3 times
        optimize(spark, tmpDir, 3)

        // Overwrite table
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)

        val indexedTable = QbeastContext.indexedTableFactory.getIndexedTable(QTableID(tmpDir))
        indexedTable.analyze(1) shouldBe Seq(CubeId.root(2).string)

      }
  }
}
