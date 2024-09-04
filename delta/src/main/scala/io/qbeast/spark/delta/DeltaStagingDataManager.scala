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
package io.qbeast.spark.delta

import io.qbeast.core.model.{IndexStatus, QTableID, StagingDataManager, StagingResolution}
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.internal.QbeastOptions
import org.apache.hadoop.fs.Path
import org.apache.spark.qbeast.config.STAGING_SIZE_IN_BYTES
import org.apache.spark.sql.delta.actions.{FileAction, RemoveFile}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * Access point for staged data
 */
private[spark] class DeltaStagingDataManager(tableID: QTableID)
  extends DeltaStagingUtils
    with StagingDataManager[FileAction] {

  private val spark = SparkSession.active

  protected override val snapshot: Snapshot =
    DeltaLog.forTable(spark, tableID.id).unsafeVolatileSnapshot

  private def stagingRemoveFiles: Seq[RemoveFile] = {
    import spark.implicits._
    stagingFiles().map(a => a.remove).as[RemoveFile].collect()
  }

  private def currentStagingSize(): Long = {
    val row = stagingFiles().selectExpr("sum(size)").first()
    if (row.isNullAt(0)) 0L
    else row.getLong(0)
  }

  /**
   * Stack a given DataFrame with all staged data.
   */
  override def mergeWithStagingData(data: DataFrame, stagedFiles: Seq[FileAction]): DataFrame = {
    if (stagedFiles.isEmpty) data
    else {
      val paths = stagedFiles.map(r => new Path(tableID.id, r.path).toString)
      val stagingData = spark.read.parquet(paths: _*)
      data.unionByName(stagingData, allowMissingColumns = true)
    }
  }

  /**
   * Resolve write policy according to the current staging size and its desired
   * value(spark.qbeast.index.stagingSizeInBytes) among the following possibilities:
   *   1. stage the data without indexing 2. index the data while ignoring the staged data 3.
   *      index (data + staging area)
   * @param data
   *   DataFrame to write
   * @return
   *   a StagingResolution instance containing the data to write, the staging RemoveFiles, and a
   *   boolean denoting whether the data to write is to be staged or indexed.
   */
  override def updateWithStagedData(data: DataFrame): StagingResolution[FileAction] = {
    STAGING_SIZE_IN_BYTES match {
      case None =>
        // Staging option deactivated, all staged data are ignored
        StagingResolution(data, Nil, sendToStaging = false)
      case Some(stagingSize) =>
        if (isInitial) {
          StagingResolution(data, Nil, sendToStaging = stagingSize > 0L)
        } else if (currentStagingSize() >= stagingSize) {
          // Full staging area, merge current data with the staging data and mark
          // all staging AddFiles as removed
          val stagedFiles = stagingRemoveFiles
          val dataToWrite = mergeWithStagingData(data, stagedFiles)
          StagingResolution(dataToWrite, stagedFiles, sendToStaging = false)
        } else {
          // The staging area is not full, stage the data
          StagingResolution(data, Nil, sendToStaging = true)
        }
    }
  }

  /**
   * Stage the data without indexing by writing it in the delta format. If the table is not yet a
   * qbeast table, use ConvertToQbeastCommand for conversion after the write.
   *
   * @param data
   *   the data to stage
   * @param indexStatus
   *   the index status
   * @param options
   *   the options
   * @param append
   *   the operation appends data
   */
  override def stageData(
                          data: DataFrame,
                          indexStatus: IndexStatus,
                          options: QbeastOptions,
                          append: Boolean): Unit = {
    // Write data to the staging area in the delta format
    var writer = data.write
      .format("delta")
      .mode(if (append) SaveMode.Append else SaveMode.Overwrite)
    for (txnVersion <- options.txnVersion; txnAppId <- options.txnAppId) {
      writer = writer
        .option(DeltaOptions.TXN_VERSION, txnVersion)
        .option(DeltaOptions.TXN_APP_ID, txnAppId)
    }
    if (options.userMetadata.nonEmpty) {
      writer = writer
        .option(DeltaOptions.USER_METADATA_OPTION, options.userMetadata.get)
    }
    writer.save(tableID.id)

    // Convert if the table is not yet qbeast
    if (isInitial) {
      val colsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
      val dcs = indexStatus.revision.desiredCubeSize
      ConvertToQbeastCommand(s"delta.`${tableID.id}`", colsToIndex, dcs).run(spark)
    }
  }

}


