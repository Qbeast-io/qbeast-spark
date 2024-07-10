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

import io.qbeast.core.model._
import io.qbeast.spark.utils.IndexMetrics.computeMinHeight
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset

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

  def cubeCount: Long = cubeElementCountStats.count

  def blockCount: Long = blockElementCountStats.count

  def fileCount: Long = fileBytesStats.count

  def bytes: Long = fileBytesStats.avg * fileCount

  val minHeight: Int = computeMinHeight(elementCount, desiredCubeSize, dimensionCount)

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

  def apply(revision: Revision, denormalizedBlocks: Dataset[DenormalizedBlock]): IndexMetrics = {
    import denormalizedBlocks.sparkSession.implicits._

    val dimensionCount = revision.columnTransformers.size
    val desiredCubeSize = revision.desiredCubeSize

    val avgFanout =
      denormalizedBlocks
        .select("cubeId.*")
        .as[CubeId]
        .distinct()
        .transform(computeAverageFanout)
        .withColumn("id", lit(1))

    val miscDS =
      denormalizedBlocks
        .select(
          sum("blockElementCount").as("elementCount"),
          (max("cubeId.depth") + 1).as("height"),
          SizeStats.forColumn($"blockElementCount").as("blockElementCountStats"),
          lit(1).as("id"))

    val cubeElementCountStatsDS =
      denormalizedBlocks
        .groupBy("cubeId")
        .agg(sum("blockElementCount").as("cubeElementCount"))
        .select(
          SizeStats.forColumn($"cubeElementCount").as("cubeElementCountStats"),
          lit(1).as("id"))

    val fileBytesStatsDS =
      denormalizedBlocks
        .select("filePath", "fileSize")
        .distinct()
        .select(SizeStats.forColumn($"fileSize").as("fileSizeStats"), lit(1).as("id"))

    val blockCountPerCubeStatsDS =
      denormalizedBlocks
        .groupBy("cubeId")
        .agg(count("*").as("blockCountPerCube"))
        .select(
          SizeStats.forColumn($"blockCountPerCube").as("blockCountPerCubeStats"),
          lit(1).as("id"))

    val blockCountPerFileStatsDS =
      denormalizedBlocks
        .groupBy("filePath")
        .agg(count("*").as("blockCountPerFile"))
        .select(
          SizeStats.forColumn($"blockCountPerFile").as("blockCountPerFileStats"),
          lit(1).as("id"))

    val (
      elementCount,
      height,
      blockElementCountStats,
      avgFanoutOpt,
      cubeElementCountStats,
      fileBytesStats,
      blockCountPerCubeStats,
      blockCountPerFileStats) =
      miscDS
        .join(avgFanout, "id")
        .join(cubeElementCountStatsDS, "id")
        .join(fileBytesStatsDS, "id")
        .join(blockCountPerCubeStatsDS, "id")
        .join(blockCountPerFileStatsDS, "id")
        .drop("id")
        .as[(Long, Int, SizeStats, Option[Double], SizeStats, SizeStats, SizeStats, SizeStats)]
        .first()

    IndexMetrics(
      revisionId = revision.revisionID,
      elementCount = elementCount,
      dimensionCount = dimensionCount,
      desiredCubeSize = desiredCubeSize,
      indexingColumns = revision.columnTransformers.map(_.spec).mkString(","),
      height = height,
      avgFanout = round(avgFanoutOpt.getOrElse(0d), decimals = 2),
      cubeElementCountStats = cubeElementCountStats,
      blockElementCountStats = blockElementCountStats,
      fileBytesStats = fileBytesStats,
      blockCountPerCubeStats = blockCountPerCubeStats,
      blockCountPerFileStats = blockCountPerFileStats,
      innerCubeStats = computeCubeStats(denormalizedBlocks.filter(!_.isLeaf)),
      leafCubeStats = computeCubeStats(denormalizedBlocks.filter(_.isLeaf)),
      denormalizedBlocks = denormalizedBlocks)
  }

  private def computeCubeStats(denormalizedBlocks: Dataset[DenormalizedBlock]): String = {
    import denormalizedBlocks.sparkSession.implicits._
    val blockElementCountStatsDF = denormalizedBlocks
      .select(
        SizeStats.forColumn($"blockElementCount").as("blockElementCountStats"),
        lit(1).as("id"))

    val cubes = denormalizedBlocks
      .groupBy("cubeId")
      .agg(
        sum("blockElementCount").as("cubeElementCount"),
        min("maxWeight.value").as("maxWeightInt"),
        count("*").as("blockCount"))
      .toDF("cubeId", "cubeElementCount", "maxWeightInt", "blockCount")

    val cubeElementCountStatsDF =
      cubes.select(
        SizeStats.forColumn($"cubeElementCount").as("cubeElementCountStats"),
        lit(1).as("id"))

    val levelWiseCubeElementCountStats = cubes
      .groupBy("cubeId.depth")
      .agg(SizeStats.forColumn($"cubeElementCount").as("cubeElementCountStats"))
      .toDF("depth", "cubeElementCountStats")

    val levelWiseAverageWeight = cubes
      .withColumn("normalizedWeight", NormalizedWeight.fromWeightColumn($"maxWeightInt"))
      .groupBy("cubeId.depth")
      .agg(avg("normalizedWeight"))
      .toDF("depth", "avgWeight")

    val levelWiseBlockCounts = cubes
      .groupBy("cubeId.depth")
      .sum("blockCount")
      .toDF("depth", "blockCount")

//    val (cubeElementCountStats, blockElementCountStats) =
    val elementCountStats =
      cubeElementCountStatsDF
        .join(blockElementCountStatsDF, "id")
        .drop("id")
        .as[(SizeStats, SizeStats)]
    elementCountStats.show(false)

    val levelWiseStats = levelWiseCubeElementCountStats
      .join(levelWiseBlockCounts, "depth")
      .join(levelWiseAverageWeight, "depth")
      .orderBy("depth")
      .select(
        $"depth",
        $"cubeElementCountStats.avg".as("avgCubeElementCount"),
        $"cubeElementCountStats.count".as("cubeCount"),
        $"blockCount",
        $"cubeElementCountStats.stddev".as("cubeElementCountStddev"),
        $"cubeElementCountStats.quartiles".as("cubeElementCountQuartiles"),
        $"avgWeight")
      .as[(Int, Long, Long, Long, Long, Seq[Long], Double)]
    levelWiseStats.show(false)

    ""
  }

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
  def computeMinHeight(elementCount: Long, desiredCubeSize: Int, dimensionCount: Int): Int = {
    // Geometric sum: Sn = a * (1 - r^n)/(1 - r) = a + ar + ar^2 + ... + ar^(n-1)
    // Sn: cube count, a: 1, r: 2^dimensionCount, n: h
    // h = log(Sn * (r - 1) / a + 1) / log(r)
    assert(elementCount >= 0, "elementCount must be non-negative")
    val sn = math.ceil(elementCount / desiredCubeSize.toDouble)
    val r = math.pow(2, dimensionCount).toInt
    val a = 1
    val h = math.ceil(math.log(sn * (r - 1) / a + 1) / math.log(r)).toInt
    h
  }

  /**
   * Compute the average fanout of a set of cubes, i.e., the average number of children per cube.
   */
  def computeAverageFanout: Dataset[CubeId] => Dataset[Option[Double]] = { cubesDS =>
    import cubesDS.sparkSession.implicits._
    cubesDS
      .groupByKey(_.parent)
      .count()
      .toDF("parent", "fanout")
      .filter("parent IS NOT NULL")
      .agg(avg($"fanout").as("avgFanout"))
      .as[Option[Double]]
  }

  def round(value: Double, decimals: Int): Double = {
    val precision = math.pow(10, decimals)
    (value * precision).toLong.toDouble / precision
  }

}

case class SizeStats(count: Long, avg: Long, stddev: Long, quartiles: Seq[Long]) {

  override def toString: String = {
    s"(count: $count, avg: $avg, stddev: $stddev, quartiles: ${quartiles.mkString("[", ",", "]")})"
  }

}

object SizeStats {

  def forColumn(column: Column): Column = {
    struct(
      count(column).as("count"),
      avg(column).cast(LongType).as("avg"),
      when(stddev(column).isNull, lit(0d))
        .otherwise(stddev(column))
        .cast(LongType)
        .as("stddev"),
      approx_percentile(column, array(lit(0), lit(0.25), lit(0.5), lit(0.75), lit(1)), lit(1000))
        .as("quartiles"))
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
