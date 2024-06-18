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
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionID

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
    cubeStatuses: SortedMap[CubeId, CubeStatus]) {

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

object IndexMetrics {

  def apply(revision: Revision, cubeStatuses: SortedMap[CubeId, CubeStatus]): IndexMetrics = {
    val dimensionCount = revision.columnTransformers.size
    val desiredCubeSize = revision.desiredCubeSize

    val blocks = cubeStatuses.values.toList.flatMap(_.blocks)
    val cubes = cubeStatuses.keys.toSeq
    val files = blocks.map(_.file).distinct
    val cubeSizes = cubeStatuses.values.toList.map(_.blocks.map(_.elementCount).sum)

    val height = if (cubes.nonEmpty) cubes.maxBy(_.depth).depth + 1 else -1

    val innerCs = cubeStatuses.filterKeys(_.children.exists(cubeStatuses.contains))
    val leafCs = cubeStatuses -- innerCs.keys

    val avgFanout = IndexMathOps.averageFanout(cubeStatuses.keys.toSet)

    val elementCount = files.map(_.elementCount).sum

    IndexMetrics(
      revisionId = revision.revisionID,
      elementCount = elementCount,
      dimensionCount = dimensionCount,
      desiredCubeSize = desiredCubeSize,
      indexingColumns = revision.columnTransformers.map(_.spec).mkString(","),
      height = height,
      avgFanout = avgFanout,
      cubeElementCountStats = SizeStats.fromLongs(cubeSizes),
      blockElementCountStats = SizeStats.fromLongs(blocks.map(_.elementCount)),
      fileBytesStats = SizeStats.fromLongs(files.map(_.size)),
      blockCountPerCubeStats =
        SizeStats.fromIntegers(cubeStatuses.values.toList.map(_.blocks.size)),
      blockCountPerFileStats = SizeStats.fromIntegers(files.map(_.blocks.size)),
      innerCubeStats = computeCubeStats(innerCs),
      leafCubeStats = computeCubeStats(leafCs),
      cubeStatuses = cubeStatuses)
  }

  private def computeCubeStats(cubeStatuses: SortedMap[CubeId, CubeStatus]): String = {
    val cubeElementCountStats =
      SizeStats.fromLongs(cubeStatuses.values.toSeq.map(_.blocks.map(_.elementCount).sum))
    val blockElementCounts =
      SizeStats.fromLongs(cubeStatuses.values.toSeq.flatMap(_.blocks).map(_.elementCount))

    val depthWiseStats = cubeStatuses
      .groupBy(_._1.depth)
      .toSeq
      .sortBy(_._1)
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
  }

}

case class SizeStats(
    count: Int,
    avg: Long,
    std: Long,
    quartiles: (Long, Long, Long, Long, Long)) {

  override def toString: String = {
    s"(count: $count, avg: $avg, std: $std, quartiles: $quartiles)"
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

  def fromIntegers(nums: Seq[Int]): SizeStats = fromLongs(nums.map(_.toLong))

  def computeQuartiles(nums: Seq[Long]): (Long, Long, Long, Long, Long) = {
    if (nums.isEmpty) (-1, -1, -1, -1, -1)
    else {
      val sorted = nums.sorted
      (
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
