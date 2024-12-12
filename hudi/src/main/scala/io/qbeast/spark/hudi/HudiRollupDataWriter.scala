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
package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.spark.writer.RollupDataWriter
import io.qbeast.spark.writer.StatsTracker
import io.qbeast.IISeq
import org.apache.hudi.client.model.HoodieInternalRow
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.TaskContext

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
 * Delta implementation of DataWriter that applies rollup to compact the files.
 */
object HudiRollupDataWriter extends RollupDataWriter {

  val GLOBAL_SEQ_NO = new AtomicLong(1)
  val LOCAL_REC_NO = mutable.Map.empty[String, AtomicLong]

  override def write(
                      tableId: QTableID,
                      schema: StructType,
                      data: DataFrame,
                      tableChanges: TableChanges,
                      commitTime: String): IISeq[IndexFile] = {

    if (data.isEmpty) return Seq.empty[IndexFile].toIndexedSeq

    val statsTrackers = StatsTracker.getStatsTrackers

    val extendedData = extendDataWithFileUUID(data, tableChanges)

    // Add the required Hudi metadata columns to the schema and create an extended schema
    // by appending them to the original schema fields.
    val newColumns = Seq(
      HoodieRecord.COMMIT_TIME_METADATA_FIELD,
      HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
      HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD,
      HoodieRecord.FILENAME_METADATA_FIELD)
      .map(StructField(_, StringType, nullable = false))
    val hudiSchema = StructType(newColumns ++ schema.fields)

    val processRow = getProcessRow(commitTime)

    val filesAndStats =
      doWrite(tableId, hudiSchema, extendedData, tableChanges, statsTrackers, Some(processRow))
    filesAndStats.map(_._1)
  }

  private def getProcessRow(commitTime: String): ProcessRows = {
    // This function adds the columns required by Hudi to the given row,
    // as specified in the extended schema, and returns a tuple containing
    // the modified row and the corresponding target filename.

    val fileExtension = HoodieFileFormat.PARQUET.getFileExtension

    (row: InternalRow, fileUUID: String) => {

      // Hudi classes of interest during the write operation:
      // DataWritingSparkTask
      // HoodieBulkInsertDataInternalWriterFactory
      // HoodieBulkInsertDataInternalWriter
      // BulkInsertDataInternalWriterHelper
      // HoodieRowCreateHandle
      val ctx = TaskContext.get()
      val partId = ctx.partitionId()
      val taskId = ctx.taskAttemptId()

      // Hudi assigns a single UUID for all rows in the same partition and tracks the number
      // of different files written by that partition. In Qbeast, this number is always 0
      // because we use a unique file UUID for each file.
      val fileId = fileUUID + "-0"

      // The write token is a composite string consisting of the current task's partition ID,
      // task attempt ID, and a task epoch. In Hudi, the task epoch is always 0.
      val writeToken = partId + "-" + taskId + "-0"

      // The filename is a concatenation of the file ID, the generated write token,
      // the commit time, and the file format (Parquet in Qbeast).
      val fileName = fileId + "_" + writeToken + "_" + commitTime + fileExtension

      // Relevant code is in HoodieRowCreateHandle
      val seqId = commitTime + "_" + partId + "_" + GLOBAL_SEQ_NO.getAndIncrement

      val recNo = LOCAL_REC_NO.getOrElseUpdate(partId.toString, new AtomicLong(1))
      val recordKey = commitTime + "_" + partId + "_" + recNo.getAndIncrement

      val updatedRow = new HoodieInternalRow(
        UTF8String.fromString(commitTime),
        UTF8String.fromString(seqId),
        UTF8String.fromString(recordKey),
        UTF8String.fromString(""),
        UTF8String.fromString(fileName),
        row,
        false)

      (updatedRow, fileName)
    }
  }

}
