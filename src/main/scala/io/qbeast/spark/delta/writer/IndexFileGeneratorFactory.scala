/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionID
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TaskAttemptContextImpl
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID

/**
 * Factory for creating IndexFileGenerator instances.
 *
 * @param tableId the table identifier
 * @param schema the table schema
 * @param revisionId the index revision identifier
 * @param writerFactory the output writer factory
 * @param config the serializable Hadoop configuration
 */
private[writer] class IndexFileGeneratorFactory(
    tableId: QTableID,
    schema: StructType,
    revisionId: RevisionID,
    writerFactory: OutputWriterFactory,
    config: SerializableConfiguration)
    extends Serializable {

  /**
   * Creates a new generator for writing a new index file.
   *
   * @return a new index file generator.
   */
  def createIndexFileGenerator(): IndexFileGenerator = {
    val path = new Path(tableId.id, s"${UUID.randomUUID()}.parquet").toString()
    val jobConfig = new JobConf(config.value)
    val taskId = new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)
    val context = new TaskAttemptContextImpl(jobConfig, taskId)
    val writer = writerFactory.newInstance(path, schema, context)
    new IndexFileGenerator(writer, revisionId, config.value)
  }

}
