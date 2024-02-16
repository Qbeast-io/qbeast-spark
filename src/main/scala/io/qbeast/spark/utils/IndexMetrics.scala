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

import io.qbeast.core.model.{CubeId, CubeStatus}
import io.qbeast.spark.utils.MathOps.{l1Deviation, l2Deviation, roundToPrecision, std}

/**
 * Metrics that aim to provide an overview for a given index revision
 * @param cubeStatuses cube-wise metadata
 * @param dimensionCount the number of indexing columns
 * @param elementCount the total number of records
 * @param depth the largest cube depth
 * @param cubeCount the number of cubes
 * @param desiredCubeSize the target cube size for all inner cubes
 * @param indexingColumns columns on which the index is built
 * @param avgFanout the average number of child cubes for all inner cubes
 * @param depthOnBalance measurement for tree imbalance
 * @param innerCubeSizeMetrics stats on inner cube sizes
 */
case class IndexMetrics(
    cubeStatuses: Map[CubeId, CubeStatus],
    dimensionCount: Int,
    elementCount: Long,
    depth: Int,
    cubeCount: Int,
    desiredCubeSize: Int,
    indexingColumns: String,
    avgFanout: Double,
    depthOnBalance: Double,
    innerCubeSizeMetrics: CubeSizeMetrics) {

  /**
   * Stats on leaf cube sizes.
   */
  lazy val leafCubeSizeMetrics: CubeSizeMetrics = {
    val leafCs = cubeStatuses.filterKeys(!_.children.exists(cubeStatuses.contains))
    CubeSizeMetrics(leafCs, desiredCubeSize)
  }

  override def toString: String = {
    s"""OTree Index Metrics:
       |dimensionCount: $dimensionCount
       |elementCount: $elementCount
       |depth: $depth
       |cubeCount: $cubeCount
       |desiredCubeSize: $desiredCubeSize
       |indexingColumns: $indexingColumns
       |avgFanout: $avgFanout
       |depthOnBalance: $depthOnBalance
       |\n$innerCubeSizeMetrics
       |""".stripMargin
  }

}

case class CubeSizeMetrics(
    min: Long,
    firstQuartile: Long,
    secondQuartile: Long,
    thirdQuartile: Long,
    max: Long,
    count: Int,
    l1_dev: Double,
    l2_dev: Double,
    levelStats: String) {

  override def toString: String = {
    s"""Stats on cube sizes:
       |Quartiles:
       |- min: $min
       |- 1stQ: $firstQuartile
       |- 2ndQ: $secondQuartile
       |- 3rdQ: $thirdQuartile
       |- max: $max
       |Stats:
       |- count: $count
       |- l1_dev: $l1_dev
       |- l2_dev: $l2_dev
       |Level-wise stats:
       |$levelStats
       |""".stripMargin
  }

}

object CubeSizeMetrics {

  def apply(cubeStatuses: Map[CubeId, CubeStatus], desiredCubeSize: Int): CubeSizeMetrics = {
    if (cubeStatuses.isEmpty) CubeSizeMetrics(-1, -1, -1, -1, -1, 0, -1, -1, "")
    else {
      val cubeSizes = cubeStatuses.values.map(_.files.map(_.elementCount).sum).toSeq.sorted
      val cubeCount = cubeStatuses.size.toDouble

      val l1_dev = l1Deviation(cubeSizes, desiredCubeSize)
      val l2_dev = l2Deviation(cubeSizes, desiredCubeSize)

      val columns = Seq("level", "avgCubeSize", "stdCubeSize", "cubeCount", "avgWeight")
      val levelStats =
        cubeStatuses
          .groupBy(cs => cs._1.depth)
          .toSeq
          .sortBy(_._1)
          .map { case (level, m) =>
            val cnt = m.size
            val avgWeight = m.values.map(_.normalizedWeight).sum / cnt
            val levelCubeSizes = m.values.toSeq.map(_.files.map(_.elementCount).sum)
            val avgCubeSize = levelCubeSizes.sum / cnt
            Seq(
              level.toString,
              avgCubeSize.toString,
              std(levelCubeSizes, avgCubeSize).toString,
              cnt.toString,
              roundToPrecision(avgWeight).toString)
          }

      CubeSizeMetrics(
        cubeSizes.min,
        cubeSizes((cubeCount * 0.25).toInt),
        cubeSizes((cubeCount * 0.50).toInt),
        cubeSizes((cubeCount * 0.75).toInt),
        cubeSizes.max,
        cubeStatuses.size,
        l1_dev,
        l2_dev,
        Tabulator.format(columns +: levelStats))
    }
  }

}

object MathOps {

  /**
   * Compute the relation between the actual depth over the theoretical depth of an index
   * assuming full fanout for all inner cubes. The result can be used to measure tree imbalance.
   * @param depth actual depth of the index
   * @param cubeCount the number of actual cubes
   * @param dimensionCount the number of dimensions
   * @return
   */
  def depthOnBalance(depth: Int, cubeCount: Int, dimensionCount: Int): Double = {
    val c = math.pow(2, dimensionCount).toInt
    val theoreticalDepth = logOfBase(c, 1 - cubeCount * (1 - c)) - 1
    depth / theoreticalDepth
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
    math.sqrt(nums.map(n => (n - mean) * (n - mean)).sum / nums.size.toDouble).toLong
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
        if (size == 0) "" else ("%" + size + "s").format(cell)
      }
      .mkString(" ")
  }

}
