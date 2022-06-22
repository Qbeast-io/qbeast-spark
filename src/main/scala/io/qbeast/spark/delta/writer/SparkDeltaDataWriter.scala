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
import org.apache.spark.qbeast.config.MAX_FILE_SIZE_COMPACTION
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{QbeastThreadUtils, SerializableConfiguration}

import scala.collection.parallel.ForkJoinTaskSupport
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

    val (factory: OutputWriterFactory, serConf: SerializableConfiguration) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance()
      (
        format.prepareWrite(sparkSession, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    val qbeastColumns = QbeastColumns(qbeastData)

    val blockWriter =
      BlockWriter(
        dataPath = tableID.id,
        schema = schema,
        schemaIndex = qbeastData.schema,
        factory = factory,
        serConf = serConf,
        qbeastColumns = qbeastColumns,
        tableChanges = tableChanges)
    qbeastData
      .repartition(col(cubeColumnName))
      .queryExecution
      .executedPlan
      .execute
      .mapPartitions(blockWriter.writeRow)
      .collect()
      .toIndexedSeq

  }

  /**
   * Compact the files
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
      cubesToCompact: Map[CubeId, Seq[QbeastBlock]],
      tableChanges: TableChanges): IISeq[FileAction] = {

    val sparkSession = SparkSession.active
    val parallelJobCollection = new ParVector(cubesToCompact.toVector)

    val threadPool =
      QbeastThreadUtils.threadUtils.newForkJoinPool("Compaction", maxThreadNumber = 15)

    val (factory: OutputWriterFactory, serConf: SerializableConfiguration) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance()
      (
        format.prepareWrite(sparkSession, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    val updates =
      try {
        val forkJoinPoolTaskSupport = new ForkJoinTaskSupport(threadPool)
        parallelJobCollection.tasksupport = forkJoinPoolTaskSupport

        parallelJobCollection.flatMap { case (cubeId: CubeId, cubeBlocks) =>
          val blocksSize = cubeBlocks.map(_.size).sum
          val addFiles = if (blocksSize > 0 && blocksSize <= MAX_FILE_SIZE_COMPACTION) {

            val fileNames = cubeBlocks.map(f => new Path(tableID.id, f.path).toString)
            val data = sparkSession.read
              .format("parquet")
              .load(fileNames: _*)
              .withColumn(cubeColumnName, lit(cubeId.bytes))

            val qbeastColumns = QbeastColumns(data)

            val blockWriter = BlockWriter(
              dataPath = tableID.id,
              schema = schema,
              schemaIndex = schema,
              factory = factory,
              serConf = serConf,
              qbeastColumns = qbeastColumns,
              tableChanges = tableChanges)

            data
              .coalesce(1)
              .queryExecution
              .executedPlan
              .execute
              .mapPartitions(blockWriter.writeRow)
              .collect()
              .toIndexedSeq
          } else {
            // TODO
            Seq.empty
          }

          addFiles ++ cubeBlocks.map(b => RemoveFile(b.path, Some(System.currentTimeMillis())))

        }.seq
      } finally {
        threadPool.shutdownNow()
      }

    val addedFiles = updates.collect { case a: AddFile => a }
    val removedFiles = updates.collect { case r: RemoveFile => r }

    addedFiles ++ removedFiles

  }

}
