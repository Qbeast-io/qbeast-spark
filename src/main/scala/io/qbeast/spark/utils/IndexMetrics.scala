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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
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

  def cubeCount: Long = cubeElementCountStats.count

  def blockCount: Long = blockElementCountStats.count

  def fileCount: Long = fileBytesStats.count

  def bytes: Long = (fileBytesStats.avg * fileCount).toLong

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
 * @param minWeight
 *   The block minimum weight
 * @param maxWeight
 *   The block maximum weight
 * @param blockElementCount
 *   The number of elements in the block
 * @param blockReplicated
 *   Whether the block is replicated or not
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

  def isLeaf(cubeStatuses: SortedMap[CubeId, CubeStatus])(cubeId: CubeId): Boolean = {
    val cubesIter = cubeStatuses.iteratorFrom(cubeId)
    cubesIter.take(2).toList match {
      // cubeId is the smaller than any cube from cubeStatuses and does not belong to the map
      case Nil => true
      // cubeId is in the map but has no children
      case List((cube, _)) => cube == cubeId
      case List((cube, _), (nextCube, _)) =>
        // cubeId is in the map and check the next cube
        if (cube == cubeId) !cubeId.isAncestorOf(nextCube)
        // cubeId is not in the map and check the cube after it
        else !cubeId.isAncestorOf(cube)
    }
  }

  def denormalizedBlocks(
      revision: Revision,
      cubeStatuses: SortedMap[CubeId, CubeStatus],
      indexFilesDs: Dataset[IndexFile]): Dataset[DenormalizedBlock] = {
    val spark = indexFilesDs.sparkSession
    import spark.implicits._

    val broadcastCubeStatuses = spark.sparkContext.broadcast(cubeStatuses)
    val isLeafUDF = udf((cubeId: CubeId) => isLeaf(broadcastCubeStatuses.value)(cubeId))

    indexFilesDs
      .withColumn("block", explode(col("blocks")))
      .select(
        $"block.cubeId".as("cubeId"),
        isLeafUDF($"block.cubeId").as("isLeaf"),
        $"path".as("filePath"),
        lit(revision.revisionID).as("revisionId"),
        $"size".as("fileSize"),
        $"modificationTime".as("fileModificationTime"),
        $"block.minWeight".as("minWeight"),
        $"block.maxWeight".as("maxWeight"),
        $"block.elementCount".as("blockElementCount"),
        $"block.replicated".as("blockReplicated"))
      .as[DenormalizedBlock]
  }

  def apply(
      revision: Revision,
      cubeStatuses: SortedMap[CubeId, CubeStatus],
      indexFilesDs: Dataset[IndexFile]): IndexMetrics = {

    IndexMetrics(revision: Revision, denormalizedBlocks(revision, cubeStatuses, indexFilesDs))
  }

  def apply(revision: Revision, denormalizedBlocks: Dataset[DenormalizedBlock]): IndexMetrics = {
    import denormalizedBlocks.sparkSession.implicits._

    val dimensionCount = revision.columnTransformers.size
    val desiredCubeSize = revision.desiredCubeSize

    val avgFanout = IndexMathOps.averageFanout(denormalizedBlocks).withColumn("id", lit(1))

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
      avgFanout = IndexMathOps.round(avgFanoutOpt.getOrElse(0d), decimals = 2),
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
    ""
//
//    import denormalizedBlocks.sparkSession.implicits._
//    val cubeElementCountStats =
//      SizeStats.fromLongs(cubeStatuses.values.toSeq.map(_.blocks.map(_.elementCount).sum))
//    val blockElementCounts =
//      SizeStats.fromLongs(cubeStatuses.values.toSeq.flatMap(_.blocks).map(_.elementCount))
//
//    val depthWiseStats =
//      denormalizedBlocks.groupByKey(_.cubeId.depth).reduceGroups().map { case (depth, csMap) =>
//        var blockCount = 0
//        val cubeElementCountStats = SizeStats.fromLongs(csMap.values.toSeq.map { cs =>
//          blockCount += cs.blocks.size
//          cs.blocks.foldLeft(0L)((acc, b) => acc + b.elementCount)
//        })
//
//        val avgWeights = csMap.values.map(_.normalizedWeight).sum / csMap.size
//        Seq(
//          depth.toString,
//          cubeElementCountStats.avg.toString,
//          cubeElementCountStats.count.toString,
//          blockCount.toString,
//          cubeElementCountStats.std.toString,
//          cubeElementCountStats.quartiles.toString,
//          avgWeights.toString)
//      }
//
//    val columnNames = Seq(
//      "depth",
//      "avgCubeElementCount",
//      "cubeCount",
//      "blockCount",
//      "cubeElementCountStd",
//      "cubeElementCountQuartiles",
//      "avgWeight")
//    "cubeElementCountStats: " + cubeElementCountStats.toString + "\n" + "blockElementCountStats: " +
//      blockElementCounts.toString + "\n" + Tabulator.format(columnNames +: depthWiseStats)

  }

}

case class SizeStats(count: Long, avg: Long, stddev: Long, quartiles: Array[Long]) {

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
    val numChildren = cubeIds
      .filterNot(_.isRoot)
      .groupBy(_.parent)
      .mapValues(_.size)
    val avgFanout = numChildren.values.sum / numChildren.size.toDouble
    round(avgFanout, decimals = 2)
  }

  def averageFanout(denormalizedBlocks: Dataset[DenormalizedBlock]): Dataset[Option[Double]] = {
    import denormalizedBlocks.sparkSession.implicits._
    denormalizedBlocks
      .groupByKey(_.cubeId.parent)
      .count()
      .toDF("parent", "fanout")
      .filter("parent IS NOT NULL")
      .agg(avg($"fanout").as("avgFanout"))
      .as[Option[Double]]
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

  def stddev(nums: Seq[Long], mean: Long): Long = {
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
