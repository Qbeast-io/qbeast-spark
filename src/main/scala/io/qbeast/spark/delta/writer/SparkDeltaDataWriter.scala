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
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.stats.DeltaFileStatistics
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, WriteTaskStats}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

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

    val dataColumns = qbeastData.schema.map(_.name).filterNot(QbeastColumns.contains)
    val cleanedData = qbeastData.selectExpr(dataColumns: _*)
    val fileStatsTrackers = getDeltaOptionalTrackers(cleanedData, sparkSession, tableID)

    val writerFactory = new IndexFileWriterFactory(
      tableId = tableID,
      schema = schema,
      tableChanges = tableChanges,
      writerFactory = factory,
      trackers = statsTrackers ++ fileStatsTrackers,
      config = serConf)

    // val strategy = new LegacyWriteStrategy(tableChanges.updatedRevision, qbeastColumns)
    val strategy = new RollupWriteStrategy(writerFactory, tableChanges)

    val indexFilesAndStats = strategy.write(qbeastData)
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

  override def compact(
      tableId: QTableID,
      schema: StructType,
      revisionId: RevisionID,
      indexFiles: IISeq[IndexFile],
      desiredFileSize: Long): IISeq[FileAction] = {
    val sparkSession = SparkSession.active
    val job = Job.getInstance(sparkSession.sparkContext.hadoopConfiguration)
    val writerFactory = new ParquetFileFormat().prepareWrite(sparkSession, job, Map.empty, schema)
    val config = new SerializableConfiguration(job.getConfiguration)
    val generatorFactory =
      new IndexFileGeneratorFactory(tableId, schema, revisionId, writerFactory, config)

    val strategy = new RollupCompactStrategy(tableId, generatorFactory, desiredFileSize)
    val (createdFiles, filesToRemove) = strategy.compact(indexFiles)
    createdFiles.map(IndexFiles.toAddFile) ++ filesToRemove.map(IndexFiles.toRemoveFile)
  }

}
