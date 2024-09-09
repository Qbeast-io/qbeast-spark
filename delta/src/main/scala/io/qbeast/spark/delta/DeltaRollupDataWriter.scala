/*
 * Copyright 2024 Qbeast Analytics, S.L.
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
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.core.stats.tracker.StatsTracker
import io.qbeast.core.stats.tracker.TaskStats
import io.qbeast.core.writer.DataWriter
import io.qbeast.core.writer.DataWriterFactory
import io.qbeast.core.writer.RollupDataWriter
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.IISeq
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.stats.DeltaFileStatistics
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

import java.net.URI

/**
 * Delta implementation of DataWriter that applies rollup to compact the files.
 */
class DeltaRollupDataWriter extends RollupDataWriter[FileAction] with DeltaStatsCollectionUtils {

  override type GetCubeMaxWeight = CubeId => Weight
  override type Extract = InternalRow => (InternalRow, Weight, CubeId, CubeId)
  override type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

  override def write(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges): IISeq[FileAction] = {

    val statsTrackers = StatsTracker.getStatsTrackers()
    val fileStatsTracker = getFileStatsTracker(tableId, data)
    val trackers = statsTrackers ++ fileStatsTracker

    val filesAndStats = internalWrite(tableId, schema, data, tableChanges, trackers)
    val stats = filesAndStats.map(_._2)
    processStats(stats, statsTrackers, fileStatsTracker)
    filesAndStats
      .map(_._1)
      .map(IndexFiles.toAddFile(dataChange = true))
      .map(correctAddFileStats(fileStatsTracker))
  }

  private def processStats(
      stats: IISeq[TaskStats],
      statsTrackers: Seq[WriteJobStatsTracker],
      fileStatsTracker: Option[DeltaJobStatisticsTracker]): Unit = {
    val basicStatsBuilder = Seq.newBuilder[WriteTaskStats]
    val fileStatsBuilder = Seq.newBuilder[WriteTaskStats]
    var endTime = 0L
    stats.foreach(stats => {
      fileStatsBuilder ++= stats.writeTaskStats.filter(_.isInstanceOf[DeltaFileStatistics])
      basicStatsBuilder ++= stats.writeTaskStats.filter(_.isInstanceOf[BasicWriteTaskStats])
      endTime = math.max(endTime, stats.endTime)
    })
    val basicStats = basicStatsBuilder.result()
    val fileStats = fileStatsBuilder.result()
    statsTrackers.foreach(_.processStats(basicStats, endTime))
    fileStatsTracker.foreach(_.processStats(fileStats, endTime))
  }

  private def getFileStatsTracker(
      tableId: QTableID,
      data: DataFrame): Option[DeltaJobStatisticsTracker] = {
    val spark = data.sparkSession
    val originalColumns = data.schema.map(_.name).filterNot(QbeastColumns.contains)
    val originalData = data.selectExpr(originalColumns: _*)
    getDeltaOptionalTrackers(originalData, spark, tableId)
  }

  private def correctAddFileStats(fileStatsTracker: Option[DeltaJobStatisticsTracker])(
      file: AddFile): AddFile = {
    val path = new Path(new URI(file.path)).toString
    fileStatsTracker
      .map(_.recordedStats(path))
      .map(stats => file.copy(stats = stats))
      .getOrElse(file)
  }

  override def optimize(
      tableId: QTableID,
      schema: StructType,
      revision: Revision,
      indexStatus: IndexStatus,
      indexFiles: Dataset[IndexFile]): IISeq[FileAction] = {

    val data = loadDataFromIndexFiles(tableId, indexFiles)
    val statsTrackers = StatsTracker.getStatsTrackers()
    val fileStatsTracker = getFileStatsTracker(tableId, data)
    val trackers = statsTrackers ++ fileStatsTracker

    val filesAndStats =
      internalOptimize(tableId, schema, revision, indexStatus, indexFiles, data, trackers)
    val stats = filesAndStats.map(_._2)
    processStats(stats, statsTrackers, fileStatsTracker)
    import indexFiles.sparkSession.implicits._
    val removeFiles =
      indexFiles.map(IndexFiles.toRemoveFile(dataChange = false)).collect().toIndexedSeq
    val addFiles = filesAndStats
      .map(_._1)
      .map(IndexFiles.toAddFile(dataChange = false))
      .map(correctAddFileStats(fileStatsTracker))
    removeFiles ++ addFiles
  }

}

class DeltaRollupDataWriterFactory extends DataWriterFactory[DataFrame, StructType, FileAction] {

  override def createDataWriter(): DataWriter[DataFrame, StructType, FileAction] = {
    new DeltaRollupDataWriter()
  }

  override val format: String = "delta"
}
