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
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.writer.RollupDataWriter
import io.qbeast.spark.writer.StatsTracker
import io.qbeast.spark.writer.TaskStats
import io.qbeast.IISeq
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 * Delta implementation of DataWriter that applies rollup to compact the files.
 */
object HudiRollupDataWriter extends RollupDataWriter {

  override def write(
      tableId: QTableID,
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges,
      commitTime: String): IISeq[IndexFile] = {

    if (data.isEmpty) return Seq.empty[IndexFile].toIndexedSeq

    val statsTrackers = StatsTracker.getStatsTrackers

    val extendedData = extendDataWithFileUUID(data, tableChanges)
    val hudiData = extendDataWithHudiColumns(extendedData, commitTime)

    val hudiSchema = schema
      .add(StructField("_hoodie_commit_time", StringType, nullable = false))
      .add(StructField("_hoodie_commit_seqno", StringType, nullable = false))
      .add(StructField("_hoodie_record_key", StringType, nullable = false))
      .add(StructField("_hoodie_partition_path", StringType, nullable = false))
      .add(StructField("_hoodie_file_name", StringType, nullable = false))

    val filesAndStats =
      doWrite(tableId, hudiSchema, hudiData, tableChanges, statsTrackers)
    val stats = filesAndStats.map(_._2)

    processStats(stats, statsTrackers)

    filesAndStats
      .map(_._1)
  }

  private def processStats(
      stats: IISeq[TaskStats],
      statsTrackers: Seq[WriteJobStatsTracker]): Unit = {
    val basicStatsBuilder = Seq.newBuilder[WriteTaskStats]
    var endTime = 0L
    stats.foreach(stats => {
      basicStatsBuilder ++= stats.writeTaskStats.filter(_.isInstanceOf[BasicWriteTaskStats])
      endTime = math.max(endTime, stats.endTime)
    })
    val basicStats = basicStatsBuilder.result()
    statsTrackers.foreach(_.processStats(basicStats, endTime))
  }

  private def extendDataWithHudiColumns(
      extendedData: DataFrame,
      commitTime: String): DataFrame = {

    def generateRecordKey(timestamp: String): UserDefinedFunction =
      udf((groupdID: Int, rowId: Int) => {
        s"${timestamp}_${groupdID}_$rowId"
      })

    def generateCommitSeqno(timestamp: String): UserDefinedFunction =
      udf((groupdID: Int, rowId: Int) => {
        s"${timestamp}_${groupdID}_$rowId"
      })

    def generateFilename(timestamp: String): UserDefinedFunction =
      udf((uuid: String, groupdID: Int) => {
        val token = s"$groupdID-${groupdID + 13}-0"
        s"$uuid-0_${token}_$timestamp.parquet"
      })

    val distinctValues = extendedData
      .select(col(QbeastColumns.fileUUIDColumnName))
      .distinct()
      .collect()
      .map(_.getString(0))
      .zipWithIndex
      .toMap

    val mappingBroadcast = SparkSession.active.sparkContext.broadcast(distinctValues)
    val assignRowGroupUDF = udf((value: String) => mappingBroadcast.value.getOrElse(value, -1))

    val windowSpecGroup = Window
      .partitionBy(col(QbeastColumns.fileUUIDColumnName))
      .orderBy(col(QbeastColumns.fileUUIDColumnName))

    val dfWithIds =
      extendedData
        .withColumn("_ROW_GROUP", assignRowGroupUDF(col(QbeastColumns.fileUUIDColumnName)))
        .withColumn("_ROW_NUM", row_number().over(windowSpecGroup) - 1)

    dfWithIds
      .withColumn("_hoodie_commit_time", lit(commitTime))
      .withColumn(
        "_hoodie_commit_seqno",
        generateCommitSeqno(commitTime)(col("_ROW_GROUP"), col("_ROW_NUM")))
      .withColumn(
        "_hoodie_record_key",
        generateRecordKey(commitTime)(col("_ROW_GROUP"), col("_ROW_NUM")))
      .withColumn("_hoodie_partition_path", lit(""))
      .withColumn(
        "_hoodie_file_name",
        generateFilename(commitTime)(col(QbeastColumns.fileUUIDColumnName), col("_ROW_GROUP")))
      .withColumn(QbeastColumns.filenameColumnName, col("_hoodie_file_name"))
      .drop("_ROW_GROUP", "_ROW_NUM")
  }

}
