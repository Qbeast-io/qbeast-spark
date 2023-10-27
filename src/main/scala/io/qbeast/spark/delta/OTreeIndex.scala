/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.Block
import io.qbeast.spark.index.query.{QueryExecutor, QuerySpecBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SQLExecution

import java.net.URI

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 * @param spark spark session
 */
case class OTreeIndex(index: TahoeLogFileIndex) extends FileIndex with Logging {

  /**
   * Snapshot to analyze
   * @return the snapshot
   */
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

  /**
   * Collect Staging AddFiles from _delta_log and convert them into FileStatuses.
   * The output is merged with those built from QbeastBlocks.
   * @return
   */
  private def stagingFiles: Seq[FileStatus] = {
    qbeastSnapshot.loadStagingBlocks().collect().map { a: AddFile =>
      new FileStatus(
        /* length */ a.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ a.modificationTime,
        absolutePath(a.path))
    }
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val qbeastFileStats = matchingBlocks(partitionFilters, dataFilters)
      .map(block => (block.file))
      .map(file => (file.path, file))
      .toMap
      .values
      .map(IndexFiles.toFileStatus(index.path))
      .toArray
    val stagingStats = stagingFiles
    val fileStats = qbeastFileStats ++ stagingStats
    val sc = index.spark.sparkContext
    val execId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val pfStr = partitionFilters.map(f => f.toString).mkString(" ")
    logInfo(s"OTreeIndex partition filters (exec id ${execId}): ${pfStr}")
    val dfStr = dataFilters.map(f => f.toString).mkString(" ")
    logInfo(s"OTreeIndex data filters (exec id ${execId}): ${dfStr}")
    val allFilesCount = snapshot.allFiles.count
    val nFiltered = allFilesCount - fileStats.length
    val filteredPct = ((nFiltered * 1.0) / allFilesCount) * 100.0
    val filteredMsg = f"${nFiltered} of ${allFilesCount} (${filteredPct}%.2f%%)"
    logInfo(s"Qbeast filtered files (exec id ${execId}): ${filteredMsg}")
    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), fileStats))
  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes

  override def rootPaths: Seq[Path] = index.rootPaths

  override def partitionSchema: StructType = index.partitionSchema
}

/**
 * Companion object for OTreeIndex
 * Builds an OTreeIndex instance from the path to a table
 */
object OTreeIndex {

  def apply(spark: SparkSession, path: Path): OTreeIndex = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val unsafeVolatileSnapshot = deltaLog.update()
    val tahoe = TahoeLogFileIndex(spark, deltaLog, path, unsafeVolatileSnapshot, Seq.empty, false)
    OTreeIndex(tahoe)
  }

}

/**
 * Singleton object for EmptyIndex.
 * Used when creating a table with no data added
 */

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
