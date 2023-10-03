/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.TableChanges
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TaskAttemptContextImpl
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID

/**
 * Factory for creating index file writers.
 *
 * @param tableId the table identifier
 * @param schema the table schema
 * @param tableChanges the table changes
 * @param writerFactory the output writer factory
 * @param trackers the stats trackers
 * @param config the Hadoop configuration
 */
private[writer] class IndexFileWriterFactory(
    tableId: QTableID,
    schema: StructType,
    tableChanges: TableChanges,
    writerFactory: OutputWriterFactory,
    trackers: Seq[WriteJobStatsTracker],
    config: SerializableConfiguration)
    extends Serializable {

  /**
   * Creates a new index file writer.
   *
   * @return the new index file writer
   */
  def createIndexFileWriter(): IndexFileWriter = {
    val path = new Path(tableId.id, s"${UUID.randomUUID()}.parquet").toString()
    val jobConfig = new JobConf(config.value)
    val taskId = new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)
    val context = new TaskAttemptContextImpl(jobConfig, taskId)
    val writer = writerFactory.newInstance(path, schema, context)
    val revisionId = tableChanges.updatedRevision.revisionID
    val generator = new IndexFileGenerator(writer, revisionId, config.value)
    val taskTrackers = trackers.map(_.newTaskInstance())
    val target = new DefaultIndexFileWriter(generator, taskTrackers, tableChanges)
    val limit = tableChanges.updatedRevision.desiredCubeSize / 10
    new BufferedIndexFileWriter(target, limit)
  }

}
