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
package io.qbeast.spark.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.IndexFileBuilder
import io.qbeast.core.model.IndexFileBuilder.BlockBuilder
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.Weight
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker

import scala.collection.mutable

/**
 * Writer for writing index files.
 *
 * @param revisionId
 *   the revision identifier
 * @param output
 *   the output writer
 * @param trackers
 *   the task stats trackers
 * @param config
 *   the Hadoop configuration
 */
private[writer] class IndexFileWriter(
    revisionId: RevisionID,
    output: OutputWriter,
    trackers: Seq[WriteTaskStatsTracker],
    config: Configuration) {

  private val file: IndexFileBuilder = new IndexFileBuilder().setRevisionId(revisionId)

  private val blocks: mutable.Map[CubeId, BlockBuilder] = mutable.Map.empty

  trackers.foreach(_.newFile(output.path()))

  /**
   * Writes a given row, that should not have Qbeast specific columns.
   *
   * @param row
   *   the row to write
   * @param weight
   *   the weight
   * @param cubeId
   *   the cube identifier
   * @param cubeMaxWeight
   *   the maximum cube weight
   */
  def write(row: InternalRow, weight: Weight, cubeId: CubeId, cubeMaxWeight: Weight): Unit = {
    output.write(row)
    val block = blocks.getOrElseUpdate(
      cubeId,
      file.beginBlock().setCubeId(cubeId).setMaxWeight(cubeMaxWeight))
    block.incrementElementCount().updateMinWeight(weight)
    trackers.foreach(_.newRow(output.path(), row))
  }

  /**
   * Closes the writer and returns the written index file with the task stats.
   *
   * @return
   *   the index file and the task stats
   */
  def close(): (IndexFile, TaskStats) = {
    output.close()
    val hadoopPath = new Path(output.path())
    val status = hadoopPath.getFileSystem(config).getFileStatus(hadoopPath)
    file
      .setPath(hadoopPath.getName)
      .setSize(status.getLen)
      .setModificationTime(status.getModificationTime)
    blocks.values.foreach(_.endBlock())
    trackers.foreach(_.closeFile(output.path()))
    val time = System.currentTimeMillis()
    val stats = trackers.map(_.getFinalStats(time))
    (file.result(), TaskStats(stats, time))
  }

}
