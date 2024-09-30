/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.QTableId
import io.qbeast.core.model.RevisionId
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
    tableId: QTableId,
    schema: StructType,
    revisionId: RevisionId,
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
