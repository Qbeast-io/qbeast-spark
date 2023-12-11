/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.Block
import io.qbeast.core.model.QbeastBlock
import io.qbeast.spark.index.query.QueryExecutor
import io.qbeast.spark.index.query.QuerySpecBuilder
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StructType
import org.apache.spark.internal.Logging

import java.net.URI

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 * @param spark spark session
 */
case class OTreeIndex(index: TahoeLogFileIndex)
  extends FileIndex
    with DeltaStagingUtils
    with Logging {

  protected def snapshot: Snapshot = index.getSnapshot

  private def qbeastSnapshot = DeltaQbeastSnapshot(snapshot)

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(index.path, p)
    }
  }

  protected def matchingBlocks(
                                partitionFilters: Seq[Expression],
                                dataFilters: Seq[Expression]): Iterable[Block] = {
    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    queryExecutor.execute()
  }

  private def qbeastMatchingFiles(
                                   partitionFilters: Seq[Expression],
                                   dataFilters: Seq[Expression]): Seq[FileStatus] = {
    matchingBlocks(partitionFilters, dataFilters).map {
      case qbeastBlock: QbeastBlock =>
        new FileStatus(
          length = qbeastBlock.size,
          isDir = false,
          blockReplication = 0,
          blockSize = 1,
          modificationTime = qbeastBlock.modificationTime,
          path = absolutePath(qbeastBlock.path))
      case block =>
        IndexFiles.toFileStatus(index.path)(block.file)
    }.toSeq
  }

  private def stagingFiles(
                            partitionFilters: Seq[Expression],
                            dataFilters: Seq[Expression]): Seq[FileStatus] = {
    index
      .matchingFiles(partitionFilters, dataFilters)
      .filter(isStagingFile)
      .map { f =>
        new FileStatus(
          length = f.size,
          isDir = false,
          blockReplication = 0,
          blockSize = 1,
          modificationTime = f.modificationTime,
          path = absolutePath(f.path))
      }
  }

  override def listFiles(
                          partitionFilters: Seq[Expression],
                          dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val qbeastFileStats = qbeastMatchingFiles(partitionFilters, dataFilters)
    val stagingFileStats = stagingFiles(partitionFilters, dataFilters)
    val fileStats = qbeastFileStats ++ stagingFileStats

    val sc = index.spark.sparkContext
    val execId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val pfStr = partitionFilters.map(_.toString).mkString(" ")
    logInfo(s"OTreeIndex partition filters (exec id $execId): $pfStr")
    val dfStr = dataFilters.map(_.toString).mkString(" ")
    logInfo(s"OTreeIndex data filters (exec id $execId): $dfStr")

    val allFilesCount = snapshot.allFiles.count
    val nFiltered = allFilesCount - fileStats.length
    val filteredPct = (nFiltered.toDouble / allFilesCount) * 100.0
    val filteredMsg = f"$nFiltered of $allFilesCount (${filteredPct}%.2f%%)"
    logInfo(s"Qbeast filtered files (exec id $execId): $filteredMsg")

    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), fileStats))
  }

  override def inputFiles: Array[String] = index.inputFiles

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes

  override def rootPaths: Seq[Path] = index.rootPaths

  override def partitionSchema: StructType = index.partitionSchema
}

object OTreeIndex {
  def apply(spark: SparkSession, path: Path): OTreeIndex = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val unsafeVolatileSnapshot = deltaLog.update()
    val tahoe = TahoeLogFileIndex(spark, deltaLog, path, unsafeVolatileSnapshot, Seq.empty, false)
    OTreeIndex(tahoe)
  }
}

object EmptyIndex extends FileIndex {
  override def rootPaths: Seq[Path] = Seq.empty

  override def listFiles(
                          partitionFilters: Seq[Expression],
                          dataFilters: Seq[Expression]): Seq[PartitionDirectory] = Seq.empty

  override def inputFiles: Array[String] = Array.empty

  override def refresh(): Unit = {}

  override def sizeInBytes: Long = 0L

  override def partitionSchema: StructType = StructType(Seq.empty)
}
