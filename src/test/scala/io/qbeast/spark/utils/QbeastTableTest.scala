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
        // WRITE SOME DATA, adds revisionIDs 0 and 1
        writeTestData(revision1, columnsToIndex, cubeSize, tmpDir)

        val revision2 = revision1.withColumn("age", col("age") * 2)
        writeTestData(revision2, columnsToIndex, cubeSize, tmpDir, "append")

        val revision3 = revision1.withColumn("val2", col("val2") * 2)
        writeTestData(revision3, columnsToIndex, cubeSize, tmpDir, "append")

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        // Including the staging revision
        qbeastTable.revisionsIDs().size shouldBe 4
        qbeastTable.revisionsIDs() == Seq(0L, 1L, 2L, 3L)
      }
  }

  "getIndexMetrics" should "return index metrics" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
        metrics.elementCount shouldBe 1001
        metrics.dimensionCount shouldBe columnsToIndex.size
        metrics.desiredCubeSize shouldBe cubeSize
        // If the tree has any inner node, avgFanout cannot be < 1.0
        metrics.avgFanout shouldBe >=(1.0)
        metrics.indexingColumns shouldBe columnsToIndex.mkString(",")

        val innerCsMetrics = metrics.innerCubeSizeMetrics
        innerCsMetrics.min shouldBe <=(innerCsMetrics.firstQuartile)
        innerCsMetrics.firstQuartile shouldBe <=(innerCsMetrics.secondQuartile)
        innerCsMetrics.secondQuartile shouldBe <=(innerCsMetrics.thirdQuartile)
        innerCsMetrics.thirdQuartile shouldBe <=(innerCsMetrics.max)

        val leafCsMetrics = metrics.leafCubeSizeMetrics
        innerCsMetrics.count + leafCsMetrics.count shouldBe metrics.cubeCount

        // Cube size std for the root
        val rootSizeStd =
          metrics.innerCubeSizeMetrics.levelStats
            .split("\n")(1)
            .split(" +")(3)

        rootSizeStd shouldBe "0"
      }
  }

  it should "return proper metrics string formats" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()

        val metricsStringLines = metrics.toString.split("\n")
        val innerCsMetricsStringLines = metrics.innerCubeSizeMetrics.toString.split("\n")
        val leafCsMetricsStringLines = metrics.leafCubeSizeMetrics.toString.split("\n")

        val metricsAttributes = Seq(
          "OTree Index Metrics",
          "dimensionCount",
          "elementCount",
          "depth:",
          "cubeCount",
          "desiredCubeSize",
          "indexingColumns",
          "avgFanout",
          "depthOnBalance")

        val csMetricsAttributes = Seq(
          "Stats on cube sizes",
          "Quartiles",
          "- min",
          "- 1stQ",
          "- 2ndQ",
          "- 3rdQ",
          "- max",
          "Stats:",
          "- count",
          "- l1_dev",
          "- l2_dev",
          "Level-wise stats")
        metricsAttributes.foreach(attr => metricsStringLines.count(_.startsWith(attr)) shouldBe 1)

        csMetricsAttributes.foreach(attr => {
          innerCsMetricsStringLines.count(_.startsWith(attr)) shouldBe 1
          leafCsMetricsStringLines.count(_.startsWith(attr)) shouldBe 1
        })
      }
  }

  it should "handle single cube index correctly" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 5000 // large cube size to make sure all elements are stored in the root
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        val metrics = qbeastTable.getIndexMetrics(Some(1L))

        metrics.depth shouldBe 1
        metrics.avgFanout shouldBe 0d

        // There is no inner cube
        val innerCsMetrics = metrics.innerCubeSizeMetrics
        innerCsMetrics.count shouldBe 0
        innerCsMetrics.min shouldBe -1
        innerCsMetrics.levelStats shouldBe ""

        val leafCsMetrics = metrics.leafCubeSizeMetrics
        leafCsMetrics.count shouldBe 1
      }
    }

  it should "check" in withSparkAndTmpDir((spark, tmpDir) => {
    val df = loadTestData(spark)
    df.write
      .mode("overwrite")
      .format("qbeast")
      .option("cubeSize", "500")
      .option("columnsToIndex", "price,user_id")
      .save(tmpDir)

    val qt = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
    // scalastyle:off println
    println(qt)
  })

}
