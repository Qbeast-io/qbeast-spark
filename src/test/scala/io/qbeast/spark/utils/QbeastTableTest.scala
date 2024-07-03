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

import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import io.qbeast.TestClasses.Client3
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class QbeastTableTest extends QbeastIntegrationTestSpec {

  private def createDF(spark: SparkSession): DataFrame = {
    val rdd =
      spark.sparkContext.parallelize(
        0.to(1000)
          .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))
    spark.createDataFrame(rdd)
  }

  private def areEqual(thisStats: SizeStats, otherStats: SizeStats): Unit = {
    thisStats.count shouldBe otherStats.count
    thisStats.avg shouldBe otherStats.avg
    thisStats.stddev shouldBe otherStats.stddev
    thisStats.quartiles sameElements otherStats.quartiles shouldBe true
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
        import spark.implicits._
        val data = createDF(spark)
        val columnsToIndex = Seq("age", "val2")
        val cubeSize = 100
        writeTestData(data, columnsToIndex, cubeSize, tmpDir)

        val qt = QbeastTable.forPath(spark, tmpDir)
        val metrics = qt.getIndexMetrics(Some(1L))

        metrics.revisionId shouldBe 1L
        metrics.elementCount shouldBe 1001
        metrics.dimensionCount shouldBe columnsToIndex.size
        metrics.desiredCubeSize shouldBe cubeSize
        metrics.indexingColumns shouldBe "age:linear,val2:linear"
        metrics.height shouldBe >(1)

        // If the tree has any inner node, avgFanout cannot be < 1.0
        metrics.avgFanout shouldBe >=(1d)

        val (cubeCount, fileCount, blockCount) =
          metrics.denormalizedBlocks
            .select(count_distinct($"cubeId"), count_distinct($"filePath"), count("*"))
            .as[(Long, Long, Long)]
            .first()

        metrics.cubeElementCountStats.count shouldBe cubeCount
        metrics.blockElementCountStats.count shouldBe blockCount
        metrics.fileBytesStats.count shouldBe fileCount
        metrics.blockCountPerCubeStats.count shouldBe blockCount
        metrics.blockCountPerFileStats.count shouldBe fileCount

      }
  }

  it should "handle single cube index correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createDF(spark)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 5000 // large cube size to make sure all elements are stored in the root
      writeTestData(data, columnsToIndex, cubeSize, tmpDir)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics(Some(1L))

      metrics.revisionId shouldBe 1L
      metrics.elementCount shouldBe 1001
      metrics.dimensionCount shouldBe columnsToIndex.size
      metrics.desiredCubeSize shouldBe cubeSize
      metrics.indexingColumns shouldBe "age:linear,val2:linear"
      metrics.height shouldBe 1
      metrics.avgFanout shouldBe 0d

      areEqual(
        metrics.cubeElementCountStats,
        SizeStats(1, 1001, 0, Array(1001, 1001, 1001, 1001, 1001)))
      areEqual(
        metrics.blockElementCountStats,
        SizeStats(1, 1001, 0, Array(1001, 1001, 1001, 1001, 1001)))
      metrics.fileBytesStats.count shouldBe 1L
      areEqual(metrics.blockCountPerCubeStats, SizeStats(1, 1, 0, Array(1, 1, 1, 1, 1)))
      areEqual(metrics.blockCountPerFileStats, SizeStats(1, 1, 0, Array(1, 1, 1, 1, 1)))

  }

  it should "handle the staging revision correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      val data = createDF(spark)
      val columnsToIndex = Seq("age", "val2")
      val cubeSize = 100
      val numFiles = 10

      // Add 10 file to the staging revision
      data.repartition(numFiles).write.mode("append").format("delta").save(tmpDir)
      ConvertToQbeastCommand(s"delta.`$tmpDir`", columnsToIndex, cubeSize).run(spark)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics(Some(0L))

      metrics.revisionId shouldBe 0L
      metrics.elementCount shouldBe 0L
      metrics.dimensionCount shouldBe 2
      metrics.desiredCubeSize shouldBe 100
      metrics.indexingColumns shouldBe "age:empty,val2:empty"
      metrics.height shouldBe 1
      metrics.avgFanout shouldBe 0d

      areEqual(metrics.cubeElementCountStats, SizeStats(1L, 0, 0, Array(0, 0, 0, 0, 0)))
      areEqual(metrics.blockElementCountStats, SizeStats(10, 0, 0, Array(0, 0, 0, 0, 0)))
      metrics.fileBytesStats.count shouldBe numFiles
      areEqual(metrics.blockCountPerCubeStats, SizeStats(1, 10, 0, Array(10, 10, 10, 10, 10)))
      areEqual(metrics.blockCountPerFileStats, SizeStats(10, 1, 0, Array(1, 1, 1, 1, 1)))
  }

  "Tabulator" should "format data correctly" in {
    val input = Seq(Seq("1", "2", "3", "4"), Seq("1", "22", "333", "4444"))
    val expected = Seq(
      Seq("1", "2 ", "3  ", "4   ").mkString(" "),
      Seq("1", "22", "333", "4444").mkString(" ")).mkString("\n")

    Tabulator.format(input) shouldBe expected
    Tabulator.format(Seq.empty) shouldBe ""
  }

}
