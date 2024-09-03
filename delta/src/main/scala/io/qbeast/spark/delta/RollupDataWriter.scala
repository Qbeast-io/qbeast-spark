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
import io.qbeast.IISeq
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.core.writer.RollupDataWriter
import io.qbeast.core.stats.tracker.TaskStats
import io.qbeast.core.stats.tracker.StatsTracker
import io.qbeast.core.stats.tracker.JobStatisticsTracker

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.Path

import java.net.URI

/**
 * Delta implementation of DataWriter that applies rollup to compact the files.
 */
object DeltaRollupDataWriter extends RollupDataWriter[FileAction]
  with DeltaStatsCollectionUtils {

  override type GetCubeMaxWeight = CubeId => Weight
  override type Extract = InternalRow => (InternalRow, Weight, CubeId, CubeId)
  override type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

  override def write(
                      tableId: QTableID,
                      schema: StructType,
                      data: DataFrame,
                      tableChanges: TableChanges): IISeq[FileAction] = {
    val extendedData = extendDataWithCubeToRollup(data, tableChanges)
    val revision = tableChanges.updatedRevision
    val getCubeMaxWeight = { cubeId: CubeId =>
      tableChanges.cubeWeight(cubeId).getOrElse(Weight.MaxValue)
    }
    val statsTrackers = StatsTracker.getStatsTrackers()
    val fileStatsTracker = getFileStatsTracker(tableId, data)
    val trackers = statsTrackers ++ fileStatsTracker
    val writeRows =
      getWriteRows(tableId, schema, extendedData, revision, getCubeMaxWeight, trackers)
    val filesAndStats = extendedData
      .repartition(col(QbeastColumns.cubeToRollupColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows)
      .collect()
      .toIndexedSeq
    val stats = filesAndStats.map(_._2)
    processStats(stats, statsTrackers, fileStatsTracker)
    filesAndStats
      .map(_._1)
      .map(IndexFiles.toAddFile(dataChange = true))
      .map(correctAddFileStats(fileStatsTracker))
  }

  private def getFileStatsTracker(
                                   tableId: QTableID,
                                   data: DataFrame): Option[JobStatisticsTracker] = {
    val spark = data.sparkSession
    val originalColumns = data.schema.map(_.name).filterNot(QbeastColumns.contains)
    val originalData = data.selectExpr(originalColumns: _*)
    getDeltaOptionalTrackers(originalData, spark, tableId)
  }

  private def correctAddFileStats(fileStatsTracker: Option[JobStatisticsTracker])(
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
    val cubeMaxWeightsBroadcast =
      data.sparkSession.sparkContext.broadcast(
        indexStatus.cubesStatuses
          .mapValues(_.maxWeight)
          .map(identity))
    var extendedData = extendDataWithWeight(data, revision)
    extendedData = extendDataWithCube(extendedData, revision, cubeMaxWeightsBroadcast.value)
    extendedData = extendDataWithCubeToRollup(extendedData, revision)
    val getCubeMaxWeight = { cubeId: CubeId =>
      cubeMaxWeightsBroadcast.value.getOrElse(cubeId, Weight.MaxValue)
    }
    val statsTrackers = StatsTracker.getStatsTrackers()
    val fileStatsTracker = getFileStatsTracker(tableId, data)
    val trackers = statsTrackers ++ fileStatsTracker
    val writeRows =
      getWriteRows(tableId, schema, extendedData, revision, getCubeMaxWeight, trackers)
    val filesAndStats = extendedData
      .repartition(col(QbeastColumns.cubeToRollupColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows)
      .collect()
      .toIndexedSeq
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
