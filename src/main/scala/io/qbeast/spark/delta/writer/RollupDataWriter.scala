/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.DeltaStatsCollectionUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.delta.actions.FileAction
import io.qbeast.core.model.{IndexStatus, QTableID, TableChanges}
import io.qbeast.IISeq
import io.qbeast.spark.index.QbeastColumns
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.catalyst.InternalRow
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.Weight
import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import io.qbeast.spark.delta.IndexFiles
import org.apache.spark.sql.delta.actions.AddFile
import scala.collection.mutable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.delta.stats.DeltaFileStatistics
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.hadoop.fs.Path
import java.net.URI

/**
 * Implementation of DataWriter that applies rollup to compact the files.
 */
object RollupDataWriter
    extends DataWriter[DataFrame, StructType, FileAction]
    with DeltaStatsCollectionUtils {

  private type Extract = InternalRow => (InternalRow, CubeId, Weight, CubeId)
  private type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

  override def write(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges): IISeq[FileAction] = {
    val statsTrackers = StatsTracker.getStatsTrackers()
    val fileStatsTracker = getFileStatsTracker(tableId, data)
    val trackers = statsTrackers ++ fileStatsTracker
    val extendedData = extendDataWithCubeToRollup(data, tableChanges)
    val writeRows = getWriteRows(tableId, schema, extendedData, tableChanges, trackers)
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
      .map(IndexFiles.toAddFile())
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
      tableChanges: TableChanges,
      trackers: Seq[WriteJobStatsTracker]): WriteRows = {
    val extract = getExtract(extendedData, tableChanges)
    val writerFactory =
      getIndexFileWriterFactory(tableId, schema, extendedData, tableChanges, trackers)
    extendedRows => {
      val writers = mutable.Map.empty[CubeId, IndexFileWriter]
      extendedRows.foreach { extendedRow =>
        val (row, cubeId, weight, rollupCubeId) = extract(extendedRow)
        val writer = writers.getOrElseUpdate(rollupCubeId, writerFactory.createIndexFileWriter())
        writer.write(row, cubeId, weight)
      }
      writers.values.iterator.map(_.close())
    }
  }

  private def getExtract(extendedData: DataFrame, tableChanges: TableChanges): Extract = {
    val schema = extendedData.schema
    val qbeastColumns = QbeastColumns(extendedData)
    val extractors = (0 until schema.fields.length)
      .filterNot(qbeastColumns.contains)
      .map { i => row: InternalRow =>
        row.get(i, schema(i).dataType)
      }
      .toSeq
    extendedRow => {
      val row = InternalRow.fromSeq(extractors.map(_.apply(extendedRow)))
      val cubeIdBytes = extendedRow.getBinary(qbeastColumns.cubeColumnIndex)
      val cubeId = tableChanges.updatedRevision.createCubeId(cubeIdBytes)
      val weight = Weight(extendedRow.getInt(qbeastColumns.weightColumnIndex))
      val rollupCubeIdBytes = extendedRow.getBinary(qbeastColumns.cubeToRollupColumnIndex)
      val rollupCubeId = tableChanges.updatedRevision.createCubeId(rollupCubeIdBytes)
      (row, cubeId, weight, rollupCubeId)
    }
  }

  private def getIndexFileWriterFactory(
      tableId: QTableID,
      schema: StructType,
      extendedData: DataFrame,
      tableChanges: TableChanges,
      trackers: Seq[WriteJobStatsTracker]): IndexFileWriterFactory = {
    val session = extendedData.sparkSession
    val job = Job.getInstance(session.sparkContext.hadoopConfiguration)
    val outputFactory = new ParquetFileFormat().prepareWrite(session, job, Map.empty, schema)
    val config = new SerializableConfiguration(job.getConfiguration())
    new IndexFileWriterFactory(tableId, schema, tableChanges, outputFactory, trackers, config)
  }

  private def extendDataWithCubeToRollup(
      data: DataFrame,
      tableChanges: TableChanges): DataFrame = {
    val rollup = computeRollup(tableChanges)
    data.withColumn(
      QbeastColumns.cubeToRollupColumnName,
      getRollupCubeIdUDF(tableChanges, rollup)(col(QbeastColumns.cubeColumnName)))
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
      tableChanges: TableChanges,
      rollup: Map[CubeId, CubeId]): UserDefinedFunction = udf({ cubeIdBytes: Array[Byte] =>
    val cubeId = tableChanges.updatedRevision.createCubeId(cubeIdBytes)
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
      tableID: QTableID,
      schema: StructType,
      indexStatus: IndexStatus,
      tableChanges: TableChanges): IISeq[FileAction] = {
    throw new UnsupportedOperationException("Not implemented yet")
  }

}
