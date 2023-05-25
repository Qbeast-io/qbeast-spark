/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.spark.delta

import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.index.query.Query
import io.qbeast.spark.index.query.ResultPart
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import java.net.URI
import org.apache.spark.sql.delta.DeltaLog

/**
 * Implementation of FileIndex which uses OTree to retrieve the data.
 *
 * @param spark the Spark session
 * @param index the underlying file index based on the transaction log
 */
class OTreeFileIndex(spark: SparkSession, index: TahoeLogFileIndex)
    extends FileIndex
    with Logging {

  override def rootPaths: Seq[Path] = index.rootPaths

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val values = new GenericInternalRow(Array.empty[Any])
    val snapshot = DeltaQbeastSnapshot(index.getSnapshot)
    val query = new Query(spark, snapshot, dataFilters)
    val files = query.execute().map(resultPartToFileStatus)
    Seq(PartitionDirectory(values, files))
  }

  override def inputFiles: Array[String] = index.inputFiles

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes

  override def partitionSchema: StructType = StructType(Array.empty[StructField])

  private def resultPartToFileStatus(part: ResultPart): FileStatus = {
    val filePathWithFragment = s"${part.filePath}#${part.from},${part.to}"
    var path = new Path(new URI(filePathWithFragment))
    if (!path.isAbsolute) {
      path = new Path(index.path, path)
    }
    new FileStatus(part.fileLength, false, 0, 1L, part.fileModificationTime, path)
  }

}

/**
 * Companion object for OTreeFileIndex
 */
object OTreeFileIndex {

  /**
   * Creates a new instance for given Spark session and table path
   */
  def apply(spark: SparkSession, path: Path): OTreeFileIndex = {
    val log = DeltaLog.forTable(spark, path)
    val index = TahoeLogFileIndex(spark, log, path, log.snapshot, Seq.empty, false)
    new OTreeFileIndex(spark, index)
  }

}
