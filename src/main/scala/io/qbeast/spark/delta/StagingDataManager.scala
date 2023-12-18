/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.internal.QbeastOptions
import org.apache.hadoop.fs.Path
import org.apache.spark.qbeast.config.STAGING_SIZE_IN_BYTES
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * Access point for staged data
 */
private[spark] class StagingDataManager(tableID: QTableID) extends DeltaStagingUtils {
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
  private def mergeWithStagingData(data: DataFrame, stagedFiles: Seq[FileAction]): DataFrame = {
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
  def updateWithStagedData(data: DataFrame): StagingResolution = {
    STAGING_SIZE_IN_BYTES match {
      case None =>
        // Staging option deactivated, all staged data are ignored
        StagingResolution(data, Nil, sendToStaging = false)
      case Some(stagingSize) =>
        if (isInitial) {
          StagingResolution(data, Nil, sendToStaging = stagingSize > 0L)
        } else if (currentStagingSize >= stagingSize) {
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
  def stageData(
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
    writer.save(tableID.id)

    // Convert if the table is not yet qbeast
    if (isInitial) {
      val colsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
      val dcs = indexStatus.revision.desiredCubeSize
      ConvertToQbeastCommand(s"delta.`${tableID.id}`", colsToIndex, dcs).run(spark)
    }
  }

}

case class StagingResolution(
    dataToWrite: DataFrame,
    removeFiles: Seq[RemoveFile],
    sendToStaging: Boolean)
