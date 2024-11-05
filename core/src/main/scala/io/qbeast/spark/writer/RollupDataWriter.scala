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

import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.IISeq
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

/**
 * Implementation of DataWriter that applies rollup to compact the files.
 */
trait RollupDataWriter extends DataWriter {

  type GetCubeMaxWeight = CubeId => Weight
  type Extract = InternalRow => (InternalRow, Weight, CubeId, CubeId)
  type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]

  protected def internalWrite(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges,
      trackers: Seq[WriteJobStatsTracker]): IISeq[(IndexFile, TaskStats)] = {
    if (data.isEmpty) return Seq.empty[(IndexFile, TaskStats)].toIndexedSeq
    val extendedData = extendDataWithCubeToRollup(data, tableChanges)
    val revision = tableChanges.updatedRevision
    val getCubeMaxWeight = { cubeId: CubeId =>
      tableChanges.cubeWeight(cubeId).getOrElse(Weight.MaxValue)
    }
    val writeRows =
      getWriteRows(tableId, schema, extendedData, revision, getCubeMaxWeight, trackers)
    extendedData
      .repartition(col(QbeastColumns.cubeToRollupColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows)
      .collect()
      .toIndexedSeq
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

  def computeRollup(tableChanges: TableChanges): Map[CubeId, CubeId] = {
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

}
