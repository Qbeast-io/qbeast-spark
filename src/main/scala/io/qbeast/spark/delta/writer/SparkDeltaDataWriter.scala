/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.index.QbeastColumns
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.qbeast.config.{
  MAX_COMPACTION_FILE_SIZE_IN_BYTES,
  MIN_COMPACTION_FILE_SIZE_IN_BYTES
}
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.stats.DeltaFileStatistics
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, WriteTaskStats}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.collection.parallel.immutable.ParVector
import java.net.URI

/**
 * Spark implementation of the DataWriter interface.
 */
object SparkDeltaDataWriter
    extends DataWriter[DataFrame, StructType, FileAction]
    with DeltaStatsCollectionUtils {

  override def write(
      tableID: QTableID,
      schema: StructType,
      qbeastData: DataFrame,
      tableChanges: TableChanges): IISeq[FileAction] = {

    val sparkSession = qbeastData.sparkSession

    val job = Job.getInstance(sparkSession.sparkContext.hadoopConfiguration)
    val factory = new ParquetFileFormat().prepareWrite(sparkSession, job, Map.empty, schema)
    val serConf = new SerializableConfiguration(job.getConfiguration)
    val statsTrackers = StatsTracker.getStatsTrackers()

    // Get Stats Trackers for each file
    val qbeastColumns = QbeastColumns(qbeastData)

    val dataColumns = qbeastData.schema.map(_.name).filterNot(QbeastColumns.contains)
    val cleanedData = qbeastData.selectExpr(dataColumns: _*)
    val fileStatsTrackers = getDeltaOptionalTrackers(cleanedData, sparkSession, tableID)

    val writerFactory = new IndexFileWriterFactory(
      tablePath = tableID.id,
      schema = schema,
      extendedSchema = qbeastData.schema,
      qbeastColumns = qbeastColumns,
      tableChanges = tableChanges,
      writerFactory = factory,
      statsTrackers = statsTrackers ++ fileStatsTrackers,
      configuration = serConf)

    // val strategy = new LegacyWriteStrategy(tableChanges.updatedRevision, qbeastColumns)
    val strategy = new RollupWriteStrategy(qbeastColumns, tableChanges)

    val indexFilesAndStats = strategy.write(qbeastData, writerFactory)
    val fileActions = indexFilesAndStats.map(_._1).map(IndexFiles.toAddFile)
    val stats = indexFilesAndStats.map(_._2)

    // Process BasicWriteJobStatsTrackers
    var fileWriteTaskStats = Seq.empty[WriteTaskStats]
    var basicWriteTaskStats = Seq.empty[WriteTaskStats]
    var endTime = 0L
    stats.foreach(taskStats => {
      fileWriteTaskStats =
        fileWriteTaskStats ++ taskStats.writeTaskStats.filter(_.isInstanceOf[DeltaFileStatistics])
      basicWriteTaskStats = (basicWriteTaskStats ++
        taskStats.writeTaskStats.filter(_.isInstanceOf[BasicWriteTaskStats]))

      endTime = math.max(endTime, taskStats.endTime)
    })
    statsTrackers.foreach(_.processStats(basicWriteTaskStats, endTime))
    fileStatsTrackers.foreach(_.processStats(fileWriteTaskStats, endTime))

    // Process DeltaWriteStats
    val resultFiles = fileActions.map { a =>
      a.copy(stats = fileStatsTrackers
        .map(_.recordedStats(new Path(new URI(a.path)).toString))
        .getOrElse(a.stats))
    }

    // Return FileAction
    resultFiles

  }

  /**
   * Split the files belonging to a cube into groups
   * that lays between [MIN_FILE_SIZE_COMPACTION, MAX_FILE_SIZE_COMPACTION]
   * Based on groupFilesIntoBins from Delta Lake OptimizeTableCommand
   * @param cubeStatuses the cubes statuses present in the index
   * @return
   */
  def groupFilesToCompact(
      cubeStatuses: IISeq[(CubeId, IISeq[Block])]): IISeq[(CubeId, IISeq[Block])] = {

    // Check what cubes are suitable for compaction
    val cubesToCompact = cubeStatuses
      .map { case (cubeId, cubeBlocks) =>
        (cubeId, cubeBlocks.filter(_.file.size >= MIN_COMPACTION_FILE_SIZE_IN_BYTES))
      }
      .filter(_._2.nonEmpty)

    cubesToCompact.flatMap { case (cube, blocks) =>
      val groups = Seq.newBuilder[Seq[Block]]
      val group = Seq.newBuilder[Block]
      var count = 0L

      blocks.foreach(b => {
        if (b.file.size + count > MAX_COMPACTION_FILE_SIZE_IN_BYTES) {
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
        count += b.file.size
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
          val fileNames = cubeBlocks.map(f => new Path(tableID.id, f.file.path).toString)
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
