/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job}
import org.apache.spark.qbeast.config.MAX_FILE_SIZE_COMPACTION
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col}
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
   * Compact the files.
   * Method based on Delta Lake OptimizeTableCommand
   * Experimental: the implementation may change by using the CubeID as partition key
   * and delegating the further exeution to the underlying Format
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

    // Group the files into units of data
    // that lays between [MIN_FILE_SIZE_COMPACTION, MAX_FILE_SIZE_COMPACTION]
    // Based on groupFilesIntoBins from Delta Lake OptimizeTableCommand
    val cubesToCompactGrouped = {
      cubesToCompact.flatMap { case (cube, blocks) =>
        val groups = Seq.newBuilder[Seq[QbeastBlock]]
        val g = Seq.newBuilder[QbeastBlock]
        var count = 0L

        blocks.foreach(b => {
          if (b.size + count > MAX_FILE_SIZE_COMPACTION) {
            // If we reach the MAX_FILE_SIZE_COMPACTION limit
            // we output a group of files for that cube
            groups += g.result()
            // Clear the current group
            g.clear()
            // Add the block to the current group
            g += b
            count = b.size
          } else {
            g += b
            count += b.size
          }
        })
        groups += g.result() // Add the last group
        groups.result().map(b => (cube, b))
      }
    }

    val parallelJobCollection = new ParVector(cubesToCompactGrouped.toVector)

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
              .coalesce(1)
              .queryExecution
              .executedPlan
              .execute
              .mapPartitions(compactor.writeBlock)
              .collect()
              .toIndexedSeq
          }

        }.seq
      } finally {
        threadPool.shutdownNow()
      }

    val addedFiles = updates.collect { case a: AddFile => a }
    val removedFiles = updates.collect { case r: RemoveFile => r }

    addedFiles ++ removedFiles

  }

}
