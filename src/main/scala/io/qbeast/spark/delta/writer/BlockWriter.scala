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

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFileBuilder
import io.qbeast.core.model.IndexFileBuilder.BlockBuilder
import io.qbeast.core.model.TableChanges
import io.qbeast.core.model.Weight
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.utils.State
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TaskAttemptContextImpl
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID
import scala.collection.mutable

/**
 * BlockWriter is in charge of writing the qbeast data into files
 *
 * @param dataPath
 *   path of the table
 * @param schema
 *   schema of the original data
 * @param schemaIndex
 *   schema with qbeast metadata columns
 * @param factory
 *   output writer factory
 * @param serConf
 *   configuration to serialize the data
 * @param qbeastColumns
 *   qbeast metadata columns
 * @param tableChanges
 *   the revision of the data to write
 */
case class BlockWriter(
    dataPath: String,
    schema: StructType,
    schemaIndex: StructType,
    factory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    statsTrackers: Seq[WriteJobStatsTracker],
    qbeastColumns: QbeastColumns,
    tableChanges: TableChanges)
    extends Serializable
    with Logging {

  /**
   * Writes rows in corresponding files
   *
   * @param iter
   *   iterator of rows
   * @return
   *   the sequence of files added
   */
  def writeRow(rows: Iterator[InternalRow]): Iterator[(AddFile, TaskStats)] = {
    val revision = tableChanges.updatedRevision
    val contexts = mutable.Map.empty[CubeId, BlockContext]
    rows.foreach { row =>
      val cubeId = revision.createCubeId(row.getBinary(qbeastColumns.cubeColumnIndex))

      val state = tableChanges.cubeState(cubeId).getOrElse(State.FLOODED)
      val maxWeight = tableChanges.cubeWeight(cubeId).getOrElse(Weight.MaxValue)
      val context = contexts.getOrElseUpdate(cubeId, buildWriter(cubeId, state, maxWeight))

      // The row with only the original columns
      val cleanRow = Seq.newBuilder[Any]
      cleanRow.sizeHint(row.numFields)
      for (i <- 0 until row.numFields) {
        if (!qbeastColumns.contains(i)) {
          cleanRow += row.get(i, schemaIndex(i).dataType)
        }
      }

      // Get the weight of the row to compute the minimumWeight per block
      val rowWeight = Weight(row.getInt(qbeastColumns.weightColumnIndex))

      // Writing the data in a single file.
      val internalRow = InternalRow.fromSeq(cleanRow.result())
      context.writer.write(internalRow)
      context.blockStatsTracker.foreach(
        _.newRow(context.path.toString, internalRow)
      ) // Update statsTrackers
      context.update(rowWeight)
    }
    contexts.values.map { context =>
      context.writer.close()

      val blockStatsTracker = context.blockStatsTracker
      val path = context.path

      // Process final stats
      blockStatsTracker.foreach(_.closeFile(path.toString))
      val endTime = System.currentTimeMillis()
      val finalStats = blockStatsTracker.map(_.getFinalStats(endTime))
      val taskStats = TaskStats(finalStats, endTime)

      // Process file status
      val fileStatus = path
        .getFileSystem(serConf.value)
        .getFileStatus(path)

      val file = context.builder
        .endBlock()
        .setPath(path.getName())
        .setSize(fileStatus.getLen())
        .setModificationTime(fileStatus.getModificationTime())
        .setRevisionId(revision.revisionID)
        .result()

      logInfo(s"Adding file ${file.path}")
      logDebug(s"""Additional file information:
              |path=${file.path},
              |size=${file.size},
              |modificationTime=${file.modificationTime},
              |revision=${file.revisionId}""".stripMargin.replaceAll("\n", " "))
      val addFile = IndexFiles.toAddFile()(file)

      (addFile, taskStats)
    }.iterator
  }

  /*
   * Creates the context to write a new cube in a new file and collect stats
   * @param cubeId a cube identifier
   * @param state the status of cube
   * @return
   */
  private def buildWriter(cubeId: CubeId, state: String, maxWeight: Weight): BlockContext = {
    val blockStatsTracker = statsTrackers.map(_.newTaskInstance())
    val writtenPath = new Path(dataPath, s"${UUID.randomUUID()}.parquet")
    val writer: OutputWriter = factory.newInstance(
      writtenPath.toString,
      schema,
      new TaskAttemptContextImpl(
        new JobConf(serConf.value),
        new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))
    val builder = new IndexFileBuilder()
      .beginBlock()
      .setCubeId(cubeId)
      .setMaxWeight(maxWeight)
      .setReplicated(state == State.ANNOUNCED || state == State.REPLICATED)
    blockStatsTracker.foreach(_.newFile(writtenPath.toString)) // Update stats trackers
    new BlockContext(builder, writer, writtenPath, blockStatsTracker)
  }

  /*
   * Container class that keeps all the mutable information we need to update a
   * block when iterating over a partition.
   * @param stats the current version of the block's stats
   * @param writer an instance of the file writer
   * @param path the path of the written file
   */
  private class BlockContext(
      val builder: BlockBuilder,
      val writer: OutputWriter,
      val path: Path,
      val blockStatsTracker: Seq[WriteTaskStatsTracker])
      extends Serializable {

    def update(minWeight: Weight): Unit =
      builder.updateMinWeight(minWeight).incrementElementCount()

  }

}
