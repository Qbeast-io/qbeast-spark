/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.QTableID
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
import io.qbeast.core.model.RevisionID

/**
 * Factory for creating IndexFileWriter instances.
 *
 * @param tableId
 *   the table identifier
 * @param schema
 *   the table schema
 * @param revisionId
 *   the index revision identifier
 * @param outputFactory
 *   the output factory
 * @param trackers
 *   the stats trackers
 * @param config
 *   the Hadoop configuration in serializable form
 */
private[writer] class IndexFileWriterFactory(
    tableId: QTableID,
    schema: StructType,
    revisionId: RevisionID,
    outputFactory: OutputWriterFactory,
    trackers: Seq[WriteJobStatsTracker],
    config: SerializableConfiguration)
    extends Serializable {

  /**
   * Creates a new IndexFileWriter instance.
   *
   * @return
   *   a new IndexFileWriter instance
   */
  def createIndexFileWriter(): IndexFileWriter = {
    val path = new Path(tableId.id, s"${UUID.randomUUID()}.parquet").toString()
    val jobConfig = new JobConf(config.value)
    val taskId = new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)
    val context = new TaskAttemptContextImpl(jobConfig, taskId)
    val output = outputFactory.newInstance(path, schema, context)
    val taskTrackers = trackers.map(_.newTaskInstance())
    new IndexFileWriter(revisionId, output, taskTrackers, config.value)
  }

}
