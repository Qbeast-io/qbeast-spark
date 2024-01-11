/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.spark.index.query.QueryFiltersUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

/**
 * Default implementation of the FileIndex.
 *
 * @param target
 *   the target file index implemented by Delta
 */
class DefaultFileIndex private (target: TahoeLogFileIndex)
    extends FileIndex
    with QueryFiltersUtils
    with Logging
    with Serializable {

  override def rootPaths: Seq[Path] = target.rootPaths

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    logFilters(partitionFilters, dataFilters)
    val strategy = if (haveQbeastWeightExpression(dataFilters)) {
      SamplingListFilesStrategy
    } else {
      DefaultListFilesStrategy
    }
    strategy.listFiles(target, partitionFilters, dataFilters)
  }

  private def logFilters(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Unit = {
    val context = target.spark.sparkContext
    val execId = context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val partitionFiltersInfo = partitionFilters.map(_.toString).mkString(" ")
    logInfo(s"DefaultFileIndex partition filters (exec id ${execId}): ${partitionFiltersInfo}")
    val dataFiltersInfo = dataFilters.map(_.toString).mkString(" ")
    logInfo(s"DefaultFileIndex data filters (exec id ${execId}): ${dataFiltersInfo}")
  }

  override def inputFiles: Array[String] = target.inputFiles

  override def refresh(): Unit = target.refresh()

  override def sizeInBytes: Long = target.sizeInBytes

  override def partitionSchema: StructType = target.partitionSchema
}

/**
 * QbeastFileIndex companion object.
 */
object DefaultFileIndex {

  /**
   * Creates a new instance from given spark session and path.
   *
   * @param spark
   *   the Spark session
   * @param path
   *   the table path
   * @return
   *   a new instance
   */
  def apply(spark: SparkSession, path: Path): DefaultFileIndex = {
    val log = DeltaLog.forTable(spark, path)
    val snapshot = log.update()
    val target = TahoeLogFileIndex(spark, log, path, snapshot, Seq.empty, false)
    new DefaultFileIndex(target)
  }

}
