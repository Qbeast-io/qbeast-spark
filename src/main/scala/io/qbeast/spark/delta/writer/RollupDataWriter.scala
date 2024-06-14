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
import io.qbeast.core.model.DataWriter
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.TableChanges
import io.qbeast.core.model.Weight
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.RowUtils
import io.qbeast.spark.internal.QbeastFunctions
import io.qbeast.IISeq
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.stats.DeltaFileStatistics
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.mutable

/**
 * Implementation of DataWriter that applies rollup to compact the files.
 */
object RollupDataWriter
    extends DataWriter[DataFrame, StructType, FileAction]
    with DeltaStatsCollectionUtils {

  private type GetCubeMaxWeight = CubeId => Weight
  private type Extract = InternalRow => (InternalRow, Weight, CubeId, CubeId)
  private type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

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

  private def computeRollup(tableChanges: TableChanges): Map[CubeId, CubeId] = {
    // TODO introduce desiredFileSize in Revision and parameters
    val desiredFileSize = tableChanges.updatedRevision.desiredCubeSize
    val rollup = new Rollup(desiredFileSize.toDouble)
    tableChanges.cubeDomains.foreach { case (cubeId, domain) =>
      val minWeight = getMinWeight(tableChanges, cubeId).fraction
      val maxWeight = getMaxWeight(tableChanges, cubeId).fraction
      val size = (maxWeight - minWeight) * domain
      rollup.populate(cubeId, size)
    }
    rollup.compute()
  }

  private def getMinWeight(tableChanges: TableChanges, cubeId: CubeId): Weight = {
    cubeId.parent match {
      case Some(parentCubeId) => getMaxWeight(tableChanges, parentCubeId)
      case None => Weight.MinValue
    }
  }

  private def getMaxWeight(tableChanges: TableChanges, cubeId: CubeId): Weight = {
    tableChanges.cubeWeight(cubeId).getOrElse(Weight.MaxValue)
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

  override def compact(
      tableId: QTableID,
      schema: StructType,
      revision: Revision,
      indexStatus: IndexStatus,
      indexFiles: Dataset[IndexFile]): IISeq[FileAction] = {
    val data = loadDataFromIndexFiles(tableId, indexFiles)
    var extendedData = extendDataWithWeight(data, revision)
    extendedData = extendDataWithCube(extendedData, revision, indexStatus)
    extendedData = extendDataWithCubeToRollup(extendedData, revision)
    val getCubeMaxWeight = { cubeId: CubeId =>
      indexStatus.cubesStatuses.get(cubeId).map(_.maxWeight).getOrElse(Weight.MaxValue)
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

  private def loadDataFromIndexFiles(
      tableId: QTableID,
      indexFiles: Dataset[IndexFile]): DataFrame = {
    import indexFiles.sparkSession.implicits._
    val paths = indexFiles
      .map(file => new Path(tableId.id, file.path).toString)
      .distinct()
      .as[String]
      .collect()
      .toSeq
    SparkSession.active.read.parquet(paths: _*)
  }

  private def extendDataWithWeight(data: DataFrame, revision: Revision): DataFrame = {
    val columns = revision.columnTransformers.map(name => data(name.columnName))
    data.withColumn(QbeastColumns.weightColumnName, QbeastFunctions.qbeastHash(columns: _*))
  }

  private def extendDataWithCube(
      extendedData: DataFrame,
      revision: Revision,
      indexStatus: IndexStatus): DataFrame = {
    val spark = extendedData.sparkSession
    val cubeMaxWeightsBroadcast =
      spark.sparkContext.broadcast(
        indexStatus.cubesStatuses
          .mapValues(_.maxWeight)
          .map(identity))
    val columns = revision.columnTransformers.map(_.columnName)
    extendedData
      .withColumn(
        QbeastColumns.cubeColumnName,
        getCubeIdUDF(revision, cubeMaxWeightsBroadcast.value)(
          struct(columns.map(col): _*),
          col(QbeastColumns.weightColumnName)))
  }

  private def getCubeIdUDF(
      revision: Revision,
      cubeMaxWeights: Map[CubeId, Weight]): UserDefinedFunction =
    udf { (row: Row, weight: Int) =>
      val point = RowUtils.rowValuesToPoint(row, revision)
      val cubeId = CubeId.containers(point).find { cubeId =>
        cubeMaxWeights.get(cubeId) match {
          case Some(maxWeight) => weight <= maxWeight.value
          case None => true
        }
      }
      cubeId.get.bytes
    }

  private def extendDataWithCubeToRollup(
      extendedData: DataFrame,
      revision: Revision): DataFrame = {
    val spark = extendedData.sparkSession
    val rollupBroadcast = spark.sparkContext.broadcast(computeRollup(revision, extendedData))
    extendedData.withColumn(
      QbeastColumns.cubeToRollupColumnName,
      getRollupCubeIdUDF(revision, rollupBroadcast.value)(col(QbeastColumns.cubeColumnName)))
  }

  private def computeRollup(revision: Revision, extendedData: DataFrame): Map[CubeId, CubeId] = {
    import extendedData.sparkSession.implicits._

    val desiredFileSize = revision.desiredCubeSize
    val rollup = new Rollup(desiredFileSize)
    extendedData
      .groupBy(QbeastColumns.cubeColumnName)
      .count()
      .map { row =>
        val cubeId = revision.createCubeId(row.getAs[Array[Byte]](0))
        val cnt = row.getLong(1)
        (cubeId, cnt)
      }
      .collect()
      .foreach { case (cubeId, cnt) => rollup.populate(cubeId, cnt.toDouble) }
    rollup.compute()
  }

}
