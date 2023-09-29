/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

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
 * Factory for creating IndexFileWriter instances.
 *
 * @param tablePath the table path
 * @param schema the table schema
 * @param extendedSchema the schema of the data frame with Qbeast-specific
 * columns
 * @param tableChanges the table changes
 * @param wroterFactory the output writer factory
 * @param statsTrackers the stats trackers
 * @param configuration the configuration to create the job
 */
private[writer] class IndexFileWriterFactory0(
    tablePath: String,
    schema: StructType,
    extendedSchema: StructType,
    tableChanges: TableChanges,
    writerFactory: OutputWriterFactory,
    statsTrackers: Seq[WriteJobStatsTracker],
    configuration: SerializableConfiguration)
    extends Serializable {

  /**
   * Creates a new writer.
   *
   * @return a new writer
   */
  def newWriter(qbeastColumns: QbeastColumns): IndexFileWriter0 = {
    val path = new Path(tablePath, s"${UUID.randomUUID()}.parquet")
    val writer = newOutputWriter(path)
    val fileBuilder = newIndexFileBuilder(path, qbeastColumns)
    val statsBuilder = newTaskStatsBuilder(path)
    new IndexFileWriter0(writer, fileBuilder, statsBuilder, excludeQbeastColumns(qbeastColumns))
  }

  private def newOutputWriter(path: Path): OutputWriter = {
    val conf = new JobConf(configuration.value)
    val taskId = new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)
    val context = new TaskAttemptContextImpl(conf, taskId)
    writerFactory.newInstance(path.toString(), schema, context)
  }

  private def newIndexFileBuilder(path: Path, qbeastColumns: QbeastColumns): IndexFileBuilder0 = {
    new IndexFileBuilder0(path, tableChanges, qbeastColumns, configuration)
  }

  private def newTaskStatsBuilder(path: Path): TaskStatsBuilder = {
    new TaskStatsBuilder(path.toString(), statsTrackers.map(_.newTaskInstance()))
  }

  private def excludeQbeastColumns(qbeastColumns: QbeastColumns)(
      extendedRow: InternalRow): InternalRow = {
    val values = (0 until extendedRow.numFields)
      .filterNot(qbeastColumns.contains)
      .map(i => extendedRow.get(i, extendedSchema(i).dataType))
      .toSeq
    InternalRow.fromSeq(values)
  }

}
