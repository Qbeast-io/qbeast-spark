/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.spark.index.query.QueryExecutor
import io.qbeast.spark.index.query.QuerySpecBuilder
import io.qbeast.spark.utils.TagUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.execution.SQLExecution

/**
 * Implementation of ListFilesStrategy to be used when the query contains sampling clauses.
 */
private[delta] object SamplingListFilesStrategy
    extends ListFilesStrategy
    with Logging
    with Serializable {

  override def listFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val snapshot = target.getSnapshot
    val path = target.path
    val files = listStagingAreaFiles(target, partitionFilters, dataFilters) ++
      listIndexFiles(snapshot, path, partitionFilters, dataFilters)
    logFilteredFiles(target, snapshot, files)
    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), files))
  }

  private def listStagingAreaFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatusWithMetadata] = {
    target
      .matchingFiles(partitionFilters, dataFilters)
      .filter(isStagingAreaFile)
      .map(file =>
        new FileStatus(
          file.size,
          false,
          0,
          1,
          file.modificationTime,
          getAbsolutePath(target, file)))
      .map(file => new FileStatusWithMetadata(file, Map.empty))
  }

  private def isStagingAreaFile(file: AddFile): Boolean = file.getTag(TagUtils.revision) match {
    case Some("0") => true
    case None => true
    case _ => false
  }

  private def getAbsolutePath(target: TahoeLogFileIndex, file: AddFile): Path = {
    val path = file.toPath
    if (path.isAbsolute()) path else new Path(target.path, path)
  }

  private def listIndexFiles(
      snapshot: Snapshot,
      path: Path,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatusWithMetadata] = {
    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val queryExecutor = new QueryExecutor(querySpecBuilder, DeltaQbeastSnapshot(snapshot))
    queryExecutor
      .execute()
      .map(block => (block.file))
      .map(file => (file.path, file))
      .toMap
      .values
      .map(IndexFiles.toFileStatusWithMetadata(path))
      .toSeq
  }

  private def logFilteredFiles(
      target: TahoeLogFileIndex,
      snapshot: Snapshot,
      files: Seq[FileStatusWithMetadata]): Unit = {
    val context = target.spark.sparkContext
    val execId = context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val count = snapshot.allFiles.count
    val filteredCount = count - files.length
    val filteredPercent = (filteredCount.toDouble / count) * 100.0
    val info = f"${filteredCount} of ${count} (${filteredPercent}%.2f%%)"
    logInfo(s"Sampling filtered files (exec id ${execId}): ${info}")
  }

}
