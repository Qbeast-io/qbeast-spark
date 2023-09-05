/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.TableChanges
import io.qbeast.spark.index.QbeastColumns
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TaskAttemptContextImpl
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID

/**
 * Writer that writes given rows into a single index file.
 *
 * @param tablePath the table path
 * @param schema the original table schema
 * @param extendedSchema the original table schema extended by Qbeast columns
 * @param qbeastColumns the Qbeast columns
 * @param tableChanges the table changes
 * @param writerFactory the factory to create OutputWriter instances
 * @param statsTrackers the stats trackers
 * @param configuration the configuration
 */
class IndexFileWriter(
    tablePath: String,
    schema: StructType,
    extendedSchema: StructType,
    qbeastColumns: QbeastColumns,
    tableChanges: TableChanges,
    writerFactory: OutputWriterFactory,
    statsTrackers: Seq[WriteJobStatsTracker],
    configuration: SerializableConfiguration)
    extends Serializable {

  /**
   * Writes given rows into an index file.
   *
   * @param extendedRows the rows to write that contain Qbeast-specific fields
   * @return the written index file and the task stats
   */
  def write(extendedRows: Iterator[InternalRow]): (IndexFile, TaskStats) = {
    val path = new Path(tablePath, s"${UUID.randomUUID()}.parquet")
    val writer = newOutputWriter(path)
    val fileBuilder = newIndexFileBuilder(path)
    val statsBuilder = newTaskStatsBuilder(path)
    statsBuilder.fileOpened()
    extendedRows.foreach { extendedRow =>
      val row = excludeQbeastColumns(extendedRow)
      writer.write(row)
      fileBuilder.rowWritten(extendedRow)
      statsBuilder.rowWritten(row)
    }
    writer.close()
    fileBuilder.fileWritten()
    statsBuilder.fileWritten()
    (fileBuilder.result(), statsBuilder.result())
  }

  private def newOutputWriter(path: Path): OutputWriter = {
    val conf = new JobConf(configuration.value)
    val taskId = new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)
    val context = new TaskAttemptContextImpl(conf, taskId)
    writerFactory.newInstance(path.toString(), schema, context)
  }

  private def newIndexFileBuilder(path: Path): IndexFileBuilder = {
    new IndexFileBuilder(path, tableChanges, qbeastColumns, configuration)
  }

  private def newTaskStatsBuilder(path: Path): TaskStatsBuilder = {
    new TaskStatsBuilder(path.toString(), statsTrackers.map(_.newTaskInstance()))
  }

  private def excludeQbeastColumns(extendedRow: InternalRow): InternalRow = {
    val values = (0 until extendedRow.numFields)
      .filterNot(qbeastColumns.contains)
      .map(i => extendedRow.get(i, extendedSchema(i).dataType))
      .toSeq
    InternalRow.fromSeq(values)
  }

}
