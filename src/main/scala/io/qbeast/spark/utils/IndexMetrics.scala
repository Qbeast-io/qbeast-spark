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
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.Weight
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset

import scala.collection.immutable.SortedMap

case class IndexMetrics(
    revisionId: RevisionID,
    elementCount: Long,
    dimensionCount: Int,
    desiredCubeSize: Int,
    indexingColumns: String,
    height: Int,
    avgFanout: Double,
    cubeElementCountStats: SizeStats,
    blockElementCountStats: SizeStats,
    fileBytesStats: SizeStats,
    blockCountPerCubeStats: SizeStats,
    blockCountPerFileStats: SizeStats,
    innerCubeStats: String,
    leafCubeStats: String,
    denormalizedBlocks: Dataset[DenormalizedBlock]) {

  def cubeCount: Int = cubeElementCountStats.count

  def blockCount: Int = blockElementCountStats.count

  def fileCount: Int = fileBytesStats.count

  def bytes: Long = fileBytesStats.avg * fileCount

  val minHeight: Int = IndexMathOps.minHeight(elementCount, desiredCubeSize, dimensionCount)

  val theoreticalFanout: Double = math.pow(2, dimensionCount)

  override def toString: String = {
    val multiBlockFilesStats =
      s"""cubeElementCountStats: $cubeElementCountStats
         |blockElementCountStats: $blockElementCountStats
         |fileBytesStats: $fileBytesStats
         |blockCountPerCubeStats: $blockCountPerCubeStats
         |blockCountPerFileStats: $blockCountPerFileStats""".stripMargin

    s"""OTree Index Metrics:
       |revisionId: $revisionId
       |elementCount: $elementCount
       |dimensionCount: $dimensionCount
       |desiredCubeSize: $desiredCubeSize
       |indexingColumns: $indexingColumns
       |height: $height ($minHeight)
       |avgFanout: $avgFanout ($theoreticalFanout)
       |cubeCount: $cubeCount
       |blockCount: $blockCount
       |fileCount: $fileCount
       |bytes: $bytes
       |\nMulti-block files stats:
       |$multiBlockFilesStats
       |\nInner cubes depth-wise stats:\n$innerCubeStats
       |\nLeaf cubes depth-wise stats:\n$leafCubeStats
       |""".stripMargin
  }

}

/**
 * * This utility case class represent a block, with all the related denormalized information.
 * @param cubeId
 *   The identifier of the cube the block belongs to
 * @param filePath
 *   The path of the file the block belongs to
 * @param revisionId
 *   The revision number
 * @param fileSize
 *   The size of the file, in bytes and including other blocks.
 * @param fileModificationTime
 *   The last modification time of the file
 * @param blockMinWeight
 *   the block minimum weight
 * @param blockMaxWeight
 *   the block maximum weight
 * @param blockElementCount
 *   the number of elements in the block
 * @param blockReplicated
 *   a boolean, representing if the block is replicated
 */

case class DenormalizedBlock(
    cubeId: CubeId,
    isLeaf: Boolean,
    filePath: String,
    revisionId: RevisionID,
    fileSize: Long,
    fileModificationTime: Long,
    minWeight: Weight,
    maxWeight: Weight,
    blockElementCount: Long,
    blockReplicated: Boolean)

object IndexMetrics {

  def denormalizedBlock(
      revision: Revision,
      cubeStatuses: SortedMap[CubeId, CubeStatus],
      fileStatus: Dataset[IndexFile]): Dataset[DenormalizedBlock] = {
    // this must be external to the lambda, to avoid SerializationErrors
    ???
  }

  def apply(
      revision: Revision,
      cubeStatuses: SortedMap[CubeId, CubeStatus],
      fileStatus: Dataset[IndexFile]): IndexMetrics = {

    IndexMetrics(revision: Revision, denormalizedBlock(revision, cubeStatuses, fileStatus))
  }

  def apply(revision: Revision, denormalizedBlocks: Dataset[DenormalizedBlock]): IndexMetrics = {
    import denormalizedBlocks.sparkSession.implicits._
    val dimensionCount = revision.columnTransformers.size
    val desiredCubeSize = revision.desiredCubeSize

    val avgFanout = IndexMathOps.averageFanout(denormalizedBlocks.map(_.cubeId))

    val cubeWindow = Window.partitionBy("cubeId")
    val fileWindow = Window.partitionBy("fileName")
    val (
      elementCount,
      height,
      cubeElementCountStats,
      blockElementCountStats,
      blockCountPerCubeStats,
      fileBytesStats,
      blockCountPerFileStats) = denormalizedBlocks
      .select(
        sum("blockElementCount").as("elementCount"),
        (max("cubeId.depth") + lit(1)).as("height"),
        SizeStats
          .forColumn(sum(col("blockElementCount").over(cubeWindow)))
          .as("cubeElementCountStats"),
        SizeStats.forColumn(sum(col("blockElementCount"))).as("blockElementCountStats"),
        SizeStats.forColumn(sum(col("blockElementCount"))).as("blockCountPerCubeStats"),
        SizeStats.forColumn(sum(col("fileSize"))).as("blockCountPerCubeStats"),
        SizeStats.forColumn(sum(col("fileSize").over(fileWindow))).as("blockCountPerFileStats"))
      .as[(Long, Int, SizeStats, SizeStats, SizeStats, SizeStats, SizeStats)]
      .first()

    IndexMetrics(
      revisionId = revision.revisionID,
      elementCount = elementCount,
      dimensionCount = dimensionCount,
      desiredCubeSize = desiredCubeSize,
      indexingColumns = revision.columnTransformers.map(_.spec).mkString(","),
      height = height,
      avgFanout = avgFanout,
      cubeElementCountStats = cubeElementCountStats,
      blockElementCountStats = blockElementCountStats,
      fileBytesStats = fileBytesStats,
      blockCountPerCubeStats = blockCountPerCubeStats,
      blockCountPerFileStats = blockCountPerFileStats,
      innerCubeStats = computeCubeStats(denormalizedBlocks.filter(!_.isLeaf)),
      leafCubeStats = computeCubeStats(denormalizedBlocks.filter(_.isLeaf)).toString,
      denormalizedBlocks = denormalizedBlocks)
  }

  private def computeCubeStats( denormalizedBlocks: Dataset[DenormalizedBlock]): String = {

      ""
  /**  import denormalizedBlocks.sparkSession.implicits._
    val cubeElementCountStats =
      SizeStats.fromLongs(cubeStatuses.values.toSeq.map(_.blocks.map(_.elementCount).sum))
    val blockElementCounts =
      SizeStats.fromLongs(cubeStatuses.values.toSeq.flatMap(_.blocks).map(_.elementCount))

    val depthWiseStats = denormalizedBlocks
      .groupByKey(_.cubeId.depth)
      .reduceGroups()
      .map { case (depth, csMap) =>
        var blockCount = 0
        val cubeElementCountStats =
          SizeStats.fromLongs(csMap.values.toSeq.map { cs =>
            blockCount += cs.blocks.size
            cs.blocks.foldLeft(0L)((acc, b) => acc + b.elementCount)
          })
        val avgWeights = csMap.values.map(_.normalizedWeight).sum / csMap.size
        Seq(
          depth.toString,
          cubeElementCountStats.avg.toString,
          cubeElementCountStats.count.toString,
          blockCount.toString,
          cubeElementCountStats.std.toString,
          cubeElementCountStats.quartiles.toString,
          avgWeights.toString)
      }

    val columnNames = Seq(
      "depth",
      "avgCubeElementCount",
      "cubeCount",
      "blockCount",
      "cubeElementCountStd",
      "cubeElementCountQuartiles",
      "avgWeight")
    "cubeElementCountStats: " + cubeElementCountStats.toString + "\n" +
      "blockElementCountStats: " + blockElementCounts.toString + "\n" +
      Tabulator.format(columnNames +: depthWiseStats)
  */
  }

}

case class SizeStats(count: Int, avg: Long, std: Long, quartiles: Array[Long]) {

  override def toString: String = {
    s"(count: $count, avg: $avg, std: $std, quartiles: ${quartiles.mkString(",")} "
  }

}

object SizeStats {

  def fromLongs(nums: Seq[Long]): SizeStats = {
    if (nums.isEmpty) SizeStats(0, -1, -1, computeQuartiles(Seq.empty))
    else {
      val avg = nums.sum / nums.size
      val std = IndexMathOps.std(nums, avg)
      SizeStats(nums.size, avg, std, computeQuartiles(nums))
    }
  }

  def forColumn(column: Column) = {
    struct(
      count(column).as("count"),
      avg(column).as("avg"),
      stddev(column).as("std"),
      approx_percentile(column, array(lit(0), lit(0.25), lit(0.5), lit(0.75), lit(1)), lit(1000))
        .as("quartile"))

  }

  def fromIntegers(nums: Seq[Int]): SizeStats = fromLongs(nums.map(_.toLong))

  def computeQuartiles(nums: Seq[Long]): Array[Long] = {
    if (nums.isEmpty) Array(-1, -1, -1, -1, -1)
    else {
      val sorted = nums.sorted
      Array(
        sorted.head,
        sorted((sorted.size * 0.25).toInt),
        sorted((sorted.size * 0.50).toInt),
        sorted((sorted.size * 0.75).toInt),
        sorted.last)
    }
  }

}

object IndexMathOps {

  /**
   * Compute the theoretical height of an index assuming a balanced tree. The result can be used
   * to measure tree imbalance, where a value close to 1 indicates a balanced tree.
   *
   * @param elementCount
   *   the number of elements in the index
   * @param desiredCubeSize
   *   the desired cube size
   * @param dimensionCount
   *   the number of dimensions in the index
   * @return
   *   the theoretical height of the index assuming a balanced tree
   */
  def minHeight(elementCount: Long, desiredCubeSize: Int, dimensionCount: Int): Int = {
    // Geometric sum: Sn = a(1-pow(r, n)/(1-r) = a + ar + ar^2 + ... + ar^(n-1)
    // Sn: cube count, a: 2, r: pow(2, dimensionCount), n: h
    val sn = math.ceil(elementCount / desiredCubeSize.toDouble)
    val r = math.pow(2, dimensionCount).toInt
    val v = 1 - (sn / 2 * (1 - r))
    val h = math.ceil(logOfBase(v, r)).toInt
    h
  }

  /**
   * Compute the average fanout of a set of cubes
   * @param cubeIds
   *   the set of cube identifiers
   * @return
   */
  def averageFanout(cubeIds: Set[CubeId]): Double = {
    val innerCubes = cubeIds.filter(_.children.exists(cubeIds.contains))
    val avgFanout =
      innerCubes.toSeq.map(_.children.count(cubeIds.contains)).sum / innerCubes.size.toDouble
    round(avgFanout, decimals = 2)
  }

  def averageFanout(cubeIdsDS: Dataset[CubeId]): Double = {
    import cubeIdsDS.sparkSession.implicits._

    // Create a DataFrame with two columns: cubeId and its children
    val cubesDF =
      cubeIdsDS.flatMap(cube => cube.children.map((cube, _))).toDF("parent", "children")

    // Join the DataFrame with itself to find the inner cubes
    val innerCubesDF =
      cubesDF.as("df1").join(cubesDF.as("df2"), $"df1.children" === $"df2.parent")

    // Compute the average fanout
    val avgFanout = innerCubesDF
      .groupBy($"df1.parent")
      .agg(count($"df1.children").alias("fanout"))
      .select(avg($"fanout"))
      .as[Double]

    // Collect the result to the driver node
    avgFanout.first()
  }

  /**
   * Compute the logarithm of a value in a given base
   * @param value
   *   the value
   * @param base
   *   the base
   * @return
   */
  def logOfBase(value: Double, base: Int): Double = {
    math.log10(value) / math.log10(base)
  }

  def l1Deviation(nums: Seq[Long], target: Int): Double =
    nums
      .map(num => math.abs(num - target))
      .sum / nums.size.toDouble / target

  def l2Deviation(nums: Seq[Long], target: Int): Double =
    math.sqrt(
      nums
        .map(num => (num - target) * (num - target))
        .sum) / nums.size.toDouble / target

  def std(nums: Seq[Long], mean: Long): Long = {
    val squared_dev = nums.map(n => (n - mean) * (n - mean)).sum / nums.size.toDouble
    math.sqrt(squared_dev).toLong
  }

  /**
   * Round a double value to a given number of decimals
   * @param value
   *   the value to round
   * @param decimals
   *   the number of decimals
   * @return
   */
  def round(value: Double, decimals: Int): Double = {
    val precision = math.pow(10, decimals)
    (value * precision).toLong.toDouble / precision
  }

}

object Tabulator {

  /**
   * Format a table by pre padding each cell to the size of the longest cell in the column
   * @param table
   *   the table to format
   * @return
   */
  def format(table: Seq[Seq[String]]): String = table match {
    case Seq() => ""
    case _ =>
      val columnSizes =
        table
          .map(row => row.map(cell => if (cell == null) 0 else cell.length))
          .transpose
          .map(_.max)
      val rows = table.map(row => formatRow(row, columnSizes))
      (rows.head :: rows.tail.toList ::: Nil).mkString("\n")
  }

  def formatRow(row: Seq[String], columnSizes: Seq[Int]): String = {
    row
      .zip(columnSizes)
      .map { case (cell, size) =>
        if (size == 0) "" else ("%-" + size + "s").format(cell)
      }
      .mkString(" ")
  }

}
