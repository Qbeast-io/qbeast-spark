/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.Weight
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
import io.qbeast.core.model.IndexFileBuilder
import io.qbeast.core.model.IndexFileBuilder.BlockBuilder
import scala.collection.mutable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import io.qbeast.core.model.RevisionID

/**
 * Writer for writing index files.
 *
 * @param revisionId the revision identifier
 * @param output the output writer
 * @param trackers the task stats trackers
 * @param config the Hadoop configuration
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
   * @param row the row to write
   * @param weight the weight
   * @param cubeId the cube identifier
   * @param cubeMaxWeight the maximum cube weight
   */
  def write(row: InternalRow, weight: Weight, cubeId: CubeId, cubeMaxWeight: Weight): Unit = {
    output.write(row)
    val block = blocks.getOrElseUpdate(
      cubeId,
      file.beginBlock().setCubeId(cubeId).setMaxWeight(cubeMaxWeight))
    block.incrementElemenCount().updateMinWeight(weight)
    trackers.foreach(_.newRow(output.path(), row))
  }

  /**
   * Closes the writer and returns the written index file with the task stats.
   *
   * @return the index file and the task stats
   */
  def close(): (IndexFile, TaskStats) = {
    output.close()
    val hadoopPath = new Path(output.path())
    val status = hadoopPath.getFileSystem(config).getFileStatus(hadoopPath)
    file
      .setPath(hadoopPath.getName())
      .setSize(status.getLen())
      .setModificationTime(status.getModificationTime())
    blocks.values.foreach(_.endBlock())
    trackers.foreach(_.closeFile(output.path()))
    val time = System.currentTimeMillis()
    val stats = trackers.map(_.getFinalStats(time))
    (file.result(), TaskStats(stats, time))
  }

}