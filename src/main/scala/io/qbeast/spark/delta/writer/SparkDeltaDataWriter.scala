/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.qbeast.config.{MAX_FILE_SIZE_COMPACTION, MIN_FILE_SIZE_COMPACTION}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, count, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import scala.collection.parallel.immutable.ParVector

/**
 * Spark implementation of the DataWriter interface.
 */
object SparkDeltaDataWriter extends DataWriter[DataFrame, StructType, FileAction] {

  override def write(
      tableID: QTableID,
      schema: StructType,
      qbeastData: DataFrame,
      tableChanges: TableChanges): IISeq[FileAction] = {

    val sparkSession = qbeastData.sparkSession

    val job = Job.getInstance()
    val factory = new ParquetFileFormat().prepareWrite(sparkSession, job, Map.empty, schema)
    val serConf = new SerializableConfiguration(job.getConfiguration)

    val qbeastColumns = QbeastColumns(qbeastData)

    val dataToWrite = if (!tableChanges.isNewRevision) {
      val roller = CubeRollUp(
        qbeastData,
        tableChanges.updatedRevision.columnTransformers.size,
        (tableChanges.updatedRevision.desiredCubeSize * 0.3).toInt)
      qbeastData.withColumn(cubeColumnName, roller.cubeRollup(col(cubeColumnName)))
    } else {
      qbeastData
    }

    val blockWriter =
      BlockWriter(
        dataPath = tableID.id,
        schema = schema,
        schemaIndex = qbeastData.schema,
        factory = factory,
        serConf = serConf,
        qbeastColumns = qbeastColumns,
        tableChanges = tableChanges)

    dataToWrite
      .repartition(col(cubeColumnName))
      .queryExecution
      .executedPlan
      .execute
      .mapPartitions(blockWriter.writeRow)
      .collect()
      .toIndexedSeq

  }

  class CubeRollUp(val mapping: Map[Array[Byte], Array[Byte]]) extends Serializable {

    val cubeRollup: UserDefinedFunction =
      udf((cube: Array[Byte]) => mapping.getOrElse(cube, cube))

  }

  object CubeRollUp {

    // scalastyle:off println
    def apply(
        qbeastData: DataFrame,
        dimensionCount: Int,
        sizeThreshold: Int,
        strategy: String = "acc"): CubeRollUp = {
      val cubeMap = strategy match {
        case "simple" => simpleUpRolling(qbeastData, dimensionCount, sizeThreshold)
        case "acc" => accumulativeUpRolling(qbeastData, dimensionCount, sizeThreshold)
      }
      println(
        s"cube count after folding:" +
          s"${cubeMap.values.map(b => CubeId(dimensionCount, b)).toSet.size}")
      new CubeRollUp(cubeMap)
    }

    def getCubeSizes(qbeastData: DataFrame, dimensionCount: Int): mutable.Map[CubeId, Long] = {
      val cubeSizes = mutable.Map[CubeId, Long]()
      qbeastData
        .groupBy(cubeColumnName)
        .agg(count("*").alias("cubeSize"))
        .collect()
        .foreach(row => {
          val bytes = row.getAs[Array[Byte]](0)
          val cube = CubeId(dimensionCount, bytes)
          val size = row.getAs[Long](1)
          cubeSizes += (cube -> size)
        })

      println(s"Cube count before folding: ${cubeSizes.size}")
      cubeSizes
    }

    /**
     * Cube payload that are smaller than the threshold are allocated
     * to their parent cube directly without further considerations
     * @param qbeastData data to write
     * @param dimensionCount number of indexing columns
     * @param sizeThreshold the minimum payload size of a cube/block for it to remain where it is
     * @return
     */
    def simpleUpRolling(
        qbeastData: DataFrame,
        dimensionCount: Int,
        sizeThreshold: Int): Map[Array[Byte], Array[Byte]] = {
      val cubeSizes = getCubeSizes(qbeastData, dimensionCount)

      cubeSizes
        .filter { case (cube, size) => !cube.isRoot && size < sizeThreshold }
        .map { case (cube, _) => (cube.bytes, cube.parent.get.bytes) }
        .toMap
    }

    def accumulativeUpRolling(
        qbeastData: DataFrame,
        dimensionCount: Int,
        sizeThreshold: Int): Map[Array[Byte], Array[Byte]] = {
      val cubeSizes = getCubeSizes(qbeastData, dimensionCount)

      val cubeFoldingMap = mutable.Map[CubeId, CubeId]()
      var maxDepth = 0

      cubeSizes.keys.foreach(cube => {
        maxDepth = maxDepth.max(cube.depth)
        cubeFoldingMap += cube -> cube
      })

      val levelCubeSizes = cubeSizes.groupBy(_._1.depth)
      (maxDepth to 1 by -1)
        .filter(levelCubeSizes.contains)
        .foreach { depth =>
          // Cube sizes from level depth
          levelCubeSizes(depth)
            // Filter out those with a size >= threshold
            .filter { case (_, size) => size < sizeThreshold }
            // Group cubes by parent cube to find sibling cubes
            .groupBy { case (cube, _) => cube.parent.get }
            // Make sure the parent cube exists
            .filter { case (parent, _) => cubeSizes.contains(parent) }
            // Process cubes from the current level
            .foreach { case (parent, siblingCubeSizes) =>
              // Add children sizes to parent size
              val newSize = cubeSizes(parent) + siblingCubeSizes.values.sum
              if (newSize < sizeThreshold * 2) {
                // update parent size
                cubeSizes(parent) = newSize
                // update mapping graph edge for sibling cubes
                siblingCubeSizes.keys.foreach(c => cubeFoldingMap(c) = parent)
              }
            }
        }

      cubeFoldingMap.keys
        .map(cube => {
          var targetCube: CubeId = cubeFoldingMap(cube)
          while (cubeFoldingMap(targetCube) != targetCube) {
            targetCube = cubeFoldingMap(targetCube)
          }
          (cube.bytes, targetCube.bytes)
        })
        .toMap
    }

  }

  /**
   * Split the files belonging to a cube into groups
   * that lays between [MIN_FILE_SIZE_COMPACTION, MAX_FILE_SIZE_COMPACTION]
   * Based on groupFilesIntoBins from Delta Lake OptimizeTableCommand
   * @param cubeStatuses the cubes statuses present in the index
   * @return
   */
  def groupFilesToCompact(
      cubeStatuses: IISeq[(CubeId, IISeq[QbeastBlock])]): IISeq[(CubeId, IISeq[QbeastBlock])] = {

    // Check what cubes are suitable for compaction
    val cubesToCompact = cubeStatuses
      .map { case (cubeId, cubeBlocks) =>
        (cubeId, cubeBlocks.filter(_.size >= MIN_FILE_SIZE_COMPACTION))
      }
      .filter(_._2.nonEmpty)

    cubesToCompact.flatMap { case (cube, blocks) =>
      val groups = Seq.newBuilder[Seq[QbeastBlock]]
      val group = Seq.newBuilder[QbeastBlock]
      var count = 0L

      blocks.foreach(b => {
        if (b.size + count > MAX_FILE_SIZE_COMPACTION) {
          // If we reach the MAX_FILE_SIZE_COMPACTION limit
          // we output a group of files for that cube
          groups += group.result()
          // Clear the current group
          group.clear()
          // Clear the count
          count = 0L
        }
        // Add the block to the group
        // Sum the size of the block
        group += b
        count += b.size
      })
      groups += group.result() // Add the last group
      groups.result().map(b => (cube, b.toIndexedSeq))
    }
  }

  /**
   * Compact the files.
   * Method based on Delta Lake OptimizeTableCommand
   * Experimental: the implementation may change by using the CubeID as partition key
   * and delegating the further execution to the underlying Format
   *
   * @param tableID
   * @param schema
   * @param data
   * @param tableChanges
   * @return
   */
  override def compact(
      tableID: QTableID,
      schema: StructType,
      indexStatus: IndexStatus,
      tableChanges: TableChanges): IISeq[FileAction] = {

    val sparkSession = SparkSession.active

    val cubesToCompact = indexStatus.cubesStatuses.mapValues(_.files).toIndexedSeq
    val cubesToCompactGrouped = groupFilesToCompact(cubesToCompact)

    // scalastyle:off println
    println(s">>> Compaction happening, how many cubes to compact? ${cubesToCompactGrouped.size}")
    cubesToCompactGrouped.foreach { case (c, b) => println(s"$c, ${b.map(_.size)}") }

    val parallelJobCollection = new ParVector(cubesToCompactGrouped.toVector)

    val job = Job.getInstance()
    val factory = new ParquetFileFormat().prepareWrite(sparkSession, job, Map.empty, schema)
    val serConf = new SerializableConfiguration(job.getConfiguration)

    val updates =
      // For each cube with a set of files to compact, we build a different task
      parallelJobCollection.flatMap { case (cubeId: CubeId, cubeBlocks) =>
        if (cubeBlocks.size <= 1) { // If the number of blocks to compact is 1 or 0, we do nothing
          Seq.empty
        } else { // Otherwise
          // Get the file names for the cubeId
          val fileNames = cubeBlocks.map(f => new Path(tableID.id, f.path).toString)
          val compactor =
            Compactor(
              tableID = tableID,
              factory = factory,
              serConf = serConf,
              schema = schema,
              cubeId,
              cubeBlocks.toIndexedSeq,
              tableChanges)

          // Load the data into a DataFrame
          // and initialize a writer in the single partition
          sparkSession.read
            .format("parquet")
            .load(fileNames: _*)
            .repartition(1)
            .queryExecution
            .executedPlan
            .execute
            .mapPartitions(compactor.writeBlock)
            .collect()
            .toIndexedSeq
        }

      }.seq

    updates

  }

}
