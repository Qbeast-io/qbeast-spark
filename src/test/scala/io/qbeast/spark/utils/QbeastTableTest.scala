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

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.CubeStatus
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.QbeastTable
import io.qbeast.TestClasses.Client3
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.SortedMap

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

        val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics(Some(1L))

        metrics.revisionId shouldBe 1L
        metrics.elementCount shouldBe 1001
        metrics.dimensionCount shouldBe columnsToIndex.size
        metrics.desiredCubeSize shouldBe cubeSize
        metrics.indexingColumns shouldBe "age:linear,val2:linear"
        metrics.height shouldBe >(1)

        // If the tree has any inner node, avgFanout cannot be < 1.0
        metrics.avgFanout shouldBe >=(1d)

        val blocks = metrics.cubeStatuses.values.toSeq.flatMap(_.blocks)

        metrics.cubeElementCountStats.count shouldBe metrics.cubeStatuses.size
        metrics.blockElementCountStats.count shouldBe blocks.size
        metrics.fileBytesStats.count shouldBe blocks.map(_.file).distinct.size
        metrics.blockCountPerCubeStats.count shouldBe metrics.cubeStatuses.size
        metrics.blockCountPerFileStats.count shouldBe metrics.fileBytesStats.count

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

      // The tree has only one cube
      metrics.height shouldBe 1
      metrics.avgFanout.isNaN shouldBe true

      // There is no inner cube
      val blocks = metrics.cubeStatuses.values.toSeq.flatMap(_.blocks)

      metrics.cubeElementCountStats.count shouldBe metrics.cubeStatuses.size
      metrics.blockElementCountStats.count shouldBe blocks.size
      metrics.fileBytesStats.count shouldBe blocks.map(_.file).distinct.size
      metrics.blockCountPerCubeStats.count shouldBe metrics.cubeStatuses.size
      metrics.blockCountPerFileStats.count shouldBe metrics.fileBytesStats.count

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

      metrics.elementCount shouldBe 0

      metrics.dimensionCount shouldBe 2
      metrics.desiredCubeSize shouldBe 100
      metrics.indexingColumns shouldBe "age:empty,val2:empty"
      metrics.height shouldBe 1
      metrics.avgFanout.isNaN shouldBe true

      metrics.cubeElementCountStats.count shouldBe 1
      metrics.blockElementCountStats.count shouldBe numFiles
      metrics.fileBytesStats.count shouldBe numFiles
      metrics.blockCountPerCubeStats.count shouldBe 1
      metrics.blockCountPerFileStats.count shouldBe numFiles
  }

  "IndexMetrics" should "handle empty cubeStatuses correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      // Add 10 file to the staging revision
      createDF(spark).repartition(10).write.mode("append").format("delta").save(tmpDir)
      ConvertToQbeastCommand(s"delta.`$tmpDir`", Seq("age", "val2"), 100).run(spark)

      import org.apache.spark.sql.delta.DeltaLog
      val dl = DeltaLog.forTable(spark, tmpDir)
      val qs = DeltaQbeastSnapshot(dl.update())

      // Use an existing revision with empty cubeStatuses
      val metrics = IndexMetrics(qs.loadLatestRevision, SortedMap.empty[CubeId, CubeStatus])

      metrics.cubeStatuses.isEmpty shouldBe true
      metrics.revisionId shouldBe 0L
      metrics.height shouldBe -1
      metrics.elementCount shouldBe 0
      metrics.indexingColumns shouldBe "age:empty,val2:empty"

      metrics.cubeElementCountStats.count shouldBe 0
      metrics.blockElementCountStats.count shouldBe 0
      metrics.fileBytesStats.count shouldBe 0
      metrics.blockCountPerCubeStats.count shouldBe 0
      metrics.blockCountPerFileStats.count shouldBe 0

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
