/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionUtils.isStagingFile
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class StagingDataManager(tableID: QTableID) {
  private val spark = SparkSession.active

  private val snapshot = DeltaLog.forTable(spark, tableID.id).snapshot

  lazy val stagingFiles: Dataset[AddFile] = snapshot.allFiles.where(isStagingFile)

  lazy val stagingRemoveFiles: Seq[RemoveFile] = {
    import spark.implicits._
    stagingFiles.map(a => a.remove).as[RemoveFile].collect()
  }

  def stagingSize: Long = {
    val row = stagingFiles.selectExpr("sum(size)").first()
    if (row.isNullAt(0)) 0L
    else row.getLong(0)
  }

  def mergeWithStagingData(data: DataFrame): DataFrame = {
    val paths = stagingRemoveFiles.map(r => new Path(tableID.id, r.path).toString)
    val stagingData = spark.read.parquet(paths: _*)
    data.unionByName(stagingData)
  }

}
