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

import io.qbeast.core.model._
import io.qbeast.spark.delta.QbeastFiles
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.IISeq
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.stats.DeltaFileStatistics
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.mutable

/**
 * Implementation of DataWriter that applies rollup to compact the files.
 */
object DeltaRollupDataWriter extends DataWriter with DeltaStatsCollectionUtils {

  private type GetCubeMaxWeight = CubeId => Weight
  private type Extract = InternalRow => (InternalRow, Weight, CubeId, CubeId)
  private type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

  override def write(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges): IISeq[IndexFile] = {
    val extendedData = extendDataWithCubeToRollup(data, tableChanges)
    val revision = tableChanges.updatedRevision
    val dimensionCount = revision.transformations.length
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
      .map(QbeastFiles.toAddFile(dataChange = true))
      .map(correctAddFileStats(fileStatsTracker))
      .map(QbeastFiles.fromAddFile(dimensionCount))
  }

  private def getFileStatsTracker(
      tableId: QTableID,
      data: DataFrame): Option[DeltaJobStatisticsTracker] = {
    val spark = data.sparkSession
    val originalColumns = data.schema.map(_.name).filterNot(QbeastColumns.contains)
    val originalData = data.selectExpr(originalColumns: _*)
    getDeltaOptionalTrackers(originalData, spark, tableId)
  }

  private def getWriteRows(
      tableId: QTableID,
      schema: StructType,
      extendedData: DataFrame,
      revision: Revision,
      getCubeMaxWeight: GetCubeMaxWeight,
      trackers: Seq[WriteJobStatsTracker]): WriteRows = {
    val extract = getExtract(extendedData, revision)
    val revisionId = revision.revisionID
    val writerFactory =
      getIndexFileWriterFactory(tableId, schema, extendedData, revisionId, trackers)
    extendedRows => {
      val writers = mutable.Map.empty[CubeId, IndexFileWriter]
      extendedRows.foreach { extendedRow =>
        val (row, weight, cubeId, rollupCubeId) = extract(extendedRow)
        val cubeMaxWeight = getCubeMaxWeight(cubeId)
        val writer = writers.getOrElseUpdate(rollupCubeId, writerFactory.createIndexFileWriter())
        writer.write(row, weight, cubeId, cubeMaxWeight)
      }
      writers.values.iterator.map(_.close())
    }
  }

  private def getExtract(extendedData: DataFrame, revision: Revision): Extract = {
    val schema = extendedData.schema
    val qbeastColumns = QbeastColumns(extendedData)
    val extractors = schema.fields.indices
      .filterNot(qbeastColumns.contains)
      .map { i => row: InternalRow =>
        row.get(i, schema(i).dataType)
      }
    extendedRow => {
      val row = InternalRow.fromSeq(extractors.map(_.apply(extendedRow)))
      val weight = Weight(extendedRow.getInt(qbeastColumns.weightColumnIndex))
      val cubeIdBytes = extendedRow.getBinary(qbeastColumns.cubeColumnIndex)
      val cubeId = revision.createCubeId(cubeIdBytes)
      val rollupCubeIdBytes = extendedRow.getBinary(qbeastColumns.cubeToRollupColumnIndex)
      val rollupCubeId = revision.createCubeId(rollupCubeIdBytes)
      (row, weight, cubeId, rollupCubeId)
    }
  }

  private def getIndexFileWriterFactory(
      tableId: QTableID,
      schema: StructType,
      extendedData: DataFrame,
      revisionId: RevisionID,
      trackers: Seq[WriteJobStatsTracker]): IndexFileWriterFactory = {
    val session = extendedData.sparkSession
    val job = Job.getInstance(session.sparkContext.hadoopConfiguration)
    val outputFactory = new ParquetFileFormat().prepareWrite(session, job, Map.empty, schema)
    val config = new SerializableConfiguration(job.getConfiguration)
    new IndexFileWriterFactory(tableId, schema, revisionId, outputFactory, trackers, config)
  }

  private def extendDataWithCubeToRollup(
      data: DataFrame,
      tableChanges: TableChanges): DataFrame = {
    val rollup = computeRollup(tableChanges)
    data.withColumn(
      QbeastColumns.cubeToRollupColumnName,
      getRollupCubeIdUDF(tableChanges.updatedRevision, rollup)(col(QbeastColumns.cubeColumnName)))
  }

  private[writer] def computeRollup(tableChanges: TableChanges): Map[CubeId, CubeId] = {
    // TODO introduce desiredFileSize in Revision and parameters
    val desiredFileSize = tableChanges.updatedRevision.desiredCubeSize
    val rollup = new Rollup(desiredFileSize)
    tableChanges.deltaBlockElementCount.foreach { case (cubeId, blockSize) =>
      rollup.populate(cubeId, blockSize)
    }
    rollup.compute()
  }

  private def getRollupCubeIdUDF(
      revision: Revision,
      rollup: Map[CubeId, CubeId]): UserDefinedFunction = udf({ cubeIdBytes: Array[Byte] =>
    val cubeId = revision.createCubeId(cubeIdBytes)
    var rollupCubeId = rollup.get(cubeId)
    var parentCubeId = cubeId.parent
    while (rollupCubeId.isEmpty) {
      parentCubeId match {
        case Some(value) =>
          rollupCubeId = rollup.get(value)
          parentCubeId = value.parent
        case None => rollupCubeId = Some(cubeId)
      }
    }
    rollupCubeId.get.bytes
  })

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

  private def correctAddFileStats(fileStatsTracker: Option[DeltaJobStatisticsTracker])(
      file: AddFile): AddFile = {
    val path = new Path(new URI(file.path)).toString
    fileStatsTracker
      .map(_.recordedStats(path))
      .map(stats => file.copy(stats = stats))
      .getOrElse(file)
  }

}
