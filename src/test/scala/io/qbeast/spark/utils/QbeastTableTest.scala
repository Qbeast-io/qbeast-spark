package io.qbeast.spark.utils

import io.qbeast.TestClasses.Client3
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class QbeastTableTest extends QbeastIntegrationTestSpec {

  private def createDF(spark: SparkSession) = {
    val rdd =
      spark.sparkContext.parallelize(
        0.to(1000)
          .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))
    spark.createDataFrame(rdd)
  }

  "IndexedColumns" should "output the indexed columns" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        // WRITE SOME DATA
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.indexedColumns() shouldBe columnsToIndex
      }
  }

  "CubeSize" should "output the cube size" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val data = createDF(spark)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 100
      // WRITE SOME DATA
      writeTestData(data, columnsToIndex, cubeSize, tmpDir)

      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable.cubeSize() shouldBe cubeSize
    }
  }

  "Latest revision" should "output the latest revision available" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        // WRITE SOME DATA
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.latestRevisionID() shouldBe 1L
      }
    }

  it should "output the latest revision from all revisions" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val revision1 = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        // WRITE SOME DATA
        writeTestData(revision1, columnsToIndex, cubeSize, tmpDir)

        val revision2 = revision1.withColumn("age", col("age") * 2)
        writeTestData(revision2, columnsToIndex, cubeSize, tmpDir, "append")

        val revision3 = revision1.withColumn("val2", col("val2") * 2)
        writeTestData(revision3, columnsToIndex, cubeSize, tmpDir, "append")

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.latestRevisionID() shouldBe 3L
      }
    }

  "Revisions" should "output all available revisions" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val revision1 = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        // WRITE SOME DATA
        writeTestData(revision1, columnsToIndex, cubeSize, tmpDir)

        val revision2 = revision1.withColumn("age", col("age") * 2)
        writeTestData(revision2, columnsToIndex, cubeSize, tmpDir, "append")

        val revision3 = revision1.withColumn("val2", col("val2") * 2)
        writeTestData(revision3, columnsToIndex, cubeSize, tmpDir, "append")

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.revisionsIDs().size shouldBe 3
        qbeastTable.revisionsIDs() shouldBe Seq(1L, 2L, 3L)
      }
  }

  "Metrics" should "return index metrics" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val data = createDF(spark)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 100
      writeTestData(data, columnsToIndex, cubeSize, tmpDir)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
      val details = metrics.nonLeafCubeSizeDetails

      // scalastyle:off println
      println(metrics)
      // scalastyle:on

      metrics.elementCount shouldBe data.count()
      metrics.dimensionCount shouldBe columnsToIndex.size
      metrics.desiredCubeSize shouldBe cubeSize
      // If the tree has any inner node, avgFanout cannot be < 1.0
      metrics.avgFanout shouldBe >=(1.0)

      details.min shouldBe <=(details.firstQuartile)
      details.firstQuartile shouldBe <=(details.secondQuartile)
      details.secondQuartile shouldBe <=(details.thirdQuartile)
      details.thirdQuartile shouldBe <=(details.max)

    }
  }

  it should "single cube tree correctly" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 5000 // large cube size to make sure all elements are stored in the root
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        val metrics = qbeastTable.getIndexMetrics()

        metrics.depth shouldBe 0
        metrics.avgFanout.isNaN shouldBe true
      }
    }
}
