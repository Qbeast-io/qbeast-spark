package io.qbeast.spark.delta

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionUtils.isStagingFile
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}

case class StagingDataManager(tableID: QTableID) {
  private val spark = SparkSession.active

  private val snapshot = DeltaLog.forTable(spark, tableID.id).snapshot

  lazy val stagingFiles: Dataset[AddFile] = snapshot.allFiles.where(isStagingFile)

  lazy val stagingSize: Long = stagingFiles.selectExpr("sum(size)").first().getLong(0)

  lazy val stagingRemoveFiles: Seq[RemoveFile] = {
    import spark.implicits._
    stagingFiles.map(a => a.remove).as[RemoveFile].collect()
  }

  def mergeWithStagingData(data: DataFrame): DataFrame = {
    val paths = stagingRemoveFiles.map(r => new Path(tableID.id, r.path).toString)
    val stagingData = spark.read.parquet(paths: _*)
    data.unionByName(stagingData)
  }

}
