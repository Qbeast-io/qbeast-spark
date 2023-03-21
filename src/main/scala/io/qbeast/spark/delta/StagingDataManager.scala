/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.{IndexStatus, QTableID}
import io.qbeast.core.model.RevisionUtils.isStagingFile
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.hadoop.fs.Path
import org.apache.spark.qbeast.config.STAGING_SIZE
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * Access point for staged data
 */
case class StagingDataManager(tableID: QTableID) {
  private val spark = SparkSession.active

  private val snapshot = DeltaLog.forTable(spark, tableID.id).snapshot

  private val isInitial = snapshot.version == -1

  lazy val stagingFiles: Dataset[AddFile] = snapshot.allFiles.where(isStagingFile)

  lazy val stagingRemoveFiles: Seq[RemoveFile] = {
    import spark.implicits._
    stagingFiles.map(a => a.remove).as[RemoveFile].collect()
  }

  lazy val currentStagingSize: Long = {
    val row = stagingFiles.selectExpr("sum(size)").first()
    if (row.isNullAt(0)) 0L
    else row.getLong(0)
  }

  /**
   * Stack a given DataFrame with all staged data.
   */
  def mergeWithStagingData(data: DataFrame): DataFrame = {
    val paths = stagingRemoveFiles.map(r => new Path(tableID.id, r.path).toString)
    val stagingData = spark.read.parquet(paths: _*)
    data.unionByName(stagingData, allowMissingColumns = true)
  }

  /**
   * Resolve write policy according to the current staging size and its
   * desired value(spark.qbeast.index.stagingSize) among the following
   * possibilities:
   * 1. stage the data without indexing
   * 2. index the data while ignoring the staged data
   * 3. index (data + staging area)
   * @param data DataFrame to write
   * @return a StagingResolution instance containing the data to write, the staging
   *         RemoveFiles, and a boolean denoting whether the data to write is to be
   *         staged or indexed.
   */
  def updateWithStagedData(data: DataFrame): StagingResolution = {
    val considerStaging = STAGING_SIZE >= 0L
    // Staging option deactivated, all staged data are ignored
    if (!considerStaging) StagingResolution(data, Nil, sendToStaging = false)
    // The staging area is empty and the staging size is not negative, write to the staging area
    else if (isInitial) StagingResolution(data, Nil, sendToStaging = true)
    else if (currentStagingSize >= STAGING_SIZE) {
      // Full staging area, merge current data with the staging data and mark
      // all staging AddFiles as removed
      StagingResolution(mergeWithStagingData(data), stagingRemoveFiles, sendToStaging = false)
    } else {
      // The staging area is not full, stage the data
      StagingResolution(data, Nil, sendToStaging = true)
    }
  }

  /**
   * Stage the data without indexing by writing it in the delta format. If the table
   * is not yet a qbeast table, use ConvertToQbeastCommand for conversion after the write.
   */
  def stageData(data: DataFrame, indexStatus: IndexStatus, append: Boolean): Unit = {
    // Write data to the staging area in the delta format
    data.write
      .format("delta")
      .mode(if (append) SaveMode.Append else SaveMode.Overwrite)
      .save(tableID.id)

    // Convert if the table is not yet qbeast
    if (isInitial) {
      val spark = SparkSession.active
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
