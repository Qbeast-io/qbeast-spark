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
    depthOnBalance: Double,
    cubeElementCountStats: SizeStats,
    blockElementCountStats: SizeStats,
    fileBytesStats: SizeStats,
    blockCountPerCubeStats: SizeStats,
    blockCountPerFileStats: SizeStats,
    innerCubeStats: String,
    leafCubeStats: String,
    cubeStatuses: SortedMap[CubeId, CubeStatus]) {

  override def toString: String = {
    s"""OTree Index Metrics:
       |revisionId: $revisionId
       |elementCount: $elementCount
       |dimensionCount: $dimensionCount
       |desiredCubeSize: $desiredCubeSize
       |indexingColumns: $indexingColumns
       |height: $height
       |avgFanout: $avgFanout
       |depthOnBalance: $depthOnBalance
       |cubeCount: ${cubeElementCountStats.count}
       |blockCount: ${blockElementCountStats.count}
       |fileCount: ${fileBytesStats.count}
       |\nStats on multi-block files:
       |cubeElementCountStats: $cubeElementCountStats
       |blockElementCountStats: $blockElementCountStats
       |fileBytesStats: $fileBytesStats
       |blockCountPerCubeStats: $blockCountPerCubeStats
       |blockCountPerFileStats: $blockCountPerFileStats
       |\nInnerCubes:\n$innerCubeStats
       |\nLeafCubes:\n$leafCubeStats
       |""".stripMargin
  }

}

object IndexMetrics {

  def apply(revision: Revision, cubeStatuses: SortedMap[CubeId, CubeStatus]): IndexMetrics = {
    val dimensionCount = revision.columnTransformers.size

    val blocks = cubeStatuses.values.toList.flatMap(_.blocks)
    val cubes = cubeStatuses.keys.toSeq
    val files = blocks.map(_.file).distinct
    val cubeSizes = cubeStatuses.values.toList.map(_.blocks.map(_.elementCount).sum)

    val height = if (cubes.nonEmpty) cubes.maxBy(_.depth).depth + 1 else -1

    val innerCs = cubeStatuses.filterKeys(_.children.exists(cubeStatuses.contains))
    val leafCs = cubeStatuses.filterKeys(!_.children.exists(cubeStatuses.contains))

    val avgFanout = innerCs.keys.foldLeft(0)((acc, c) =>
      acc + c.children.count(cubeStatuses.contains)) / innerCs.size.toDouble

    val dob = MathOps.depthOnBalance(height, cubes.size, dimensionCount)
    val elementCount = files.map(_.elementCount).sum
    IndexMetrics(
      revisionId = revision.revisionID,
      elementCount = elementCount,
      dimensionCount = dimensionCount,
      desiredCubeSize = revision.desiredCubeSize,
      indexingColumns = revision.columnTransformers.map(_.spec).mkString(","),
      height = height,
      avgFanout = avgFanout,
      depthOnBalance = dob,
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

    val s = cubeStatuses
      .groupBy(_._1.depth)
      .toSeq
      .sortBy(_._1)
      .map { case (level, csMap) =>
        var blockCount = 0
        val cubeElementCountStats =
          SizeStats.fromLongs(csMap.values.toSeq.map { cs =>
            blockCount += cs.blocks.size
            cs.blocks.foldLeft(0L)((acc, b) => acc + b.elementCount)
          })
        val avgWeights = csMap.values.map(_.normalizedWeight).sum / csMap.size
        Seq(
          level.toString,
          cubeElementCountStats.avg.toString,
          cubeElementCountStats.count.toString,
          blockCount.toString,
          cubeElementCountStats.std.toString,
          cubeElementCountStats.quartiles.toString,
          avgWeights.toString)
      }

    "cubeElementCountStats: " + cubeElementCountStats.toString + "\n" +
      "blockElementCountStats: " + blockElementCounts.toString + "\n" +
      Tabulator.format(
        Seq(
          "level",
          "avgCubeElementCount",
          "cubeCount",
          "blockCount",
          "cubeElementCountStd",
          "cubeElementCountQuartiles",
          "avgWeight") +: s)
  }

}

case class SizeStats(count: Int, avg: Long, std: Long, quartiles: Quartiles) {

  override def toString: String = s"(count: $count, avg: $avg, std: $std, quartiles: $quartiles)"

}

object SizeStats {

  def fromLongs(nums: Seq[Long]): SizeStats = {
    if (nums.isEmpty) SizeStats.empty()
    else {
      val avg = nums.sum / nums.size
      val std = MathOps.std(nums, avg)
      SizeStats(nums.size, avg, std, Quartiles(nums))
    }
  }

  def fromIntegers(nums: Seq[Int]): SizeStats = fromLongs(nums.map(_.toLong))

  def empty(): SizeStats = SizeStats(0, -1, -1, Quartiles.empty())

}

case class Quartiles(min: Long, first: Long, second: Long, third: Long, max: Long) {

  override def toString: String = {
    s"($min, $first, $second, $third, $max)"
  }

}

object Quartiles {

  def apply(nums: Seq[Long]): Quartiles = {
    if (nums.isEmpty) Quartiles.empty()
    else {
      val sorted = nums.sorted
      Quartiles(
        sorted.head,
        sorted((sorted.size * 0.25).toInt),
        sorted((sorted.size * 0.50).toInt),
        sorted((sorted.size * 0.75).toInt),
        sorted.last)
    }
  }

  def empty(): Quartiles = Quartiles(-1, -1, -1, -1, -1)

}

object MathOps {

  /**
   * Compute the relation between the actual depth over the theoretical depth of an index assuming
   * full fanout for all inner cubes. The result can be used to measure tree imbalance.
   * @param height
   *   actual height of the index
   * @param cubeCount
   *   the number of actual cubes
   * @param dimensionCount
   *   the number of dimensions
   * @return
   */
  def depthOnBalance(height: Int, cubeCount: Int, dimensionCount: Int): Double = {
    val c = math.pow(2, dimensionCount).toInt
    val theoreticalHeight = logOfBase(c, 1 - cubeCount * (1 - c)) - 1
    height / theoreticalHeight
  }

  def logOfBase(base: Int, value: Double): Double = {
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

  def roundToPrecision(value: Double, digits: Int = 5): Double = {
    val precision = math.pow(10, digits)
    (value * precision).toLong.toDouble / precision
  }

}

object Tabulator {

  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val columnSizes =
        table
          .map(row => row.map(cell => if (cell == null) 0 else cell.toString.length))
          .transpose
          .map(_.max)
      val rows = table.map(row => formatRow(row, columnSizes))
      (rows.head :: rows.tail.toList ::: Nil).mkString("\n")
  }

  def formatRow(row: Seq[Any], columnSizes: Seq[Int]): String = {
    row
      .zip(columnSizes)
      .map { case (cell, size) =>
        if (size == 0) "" else ("%-" + size + "s").format(cell)
      }
      .mkString(" ")
  }

}
