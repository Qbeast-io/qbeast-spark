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
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata
import org.apache.spark.sql.execution.datasources.PartitionDirectory

/**
 * Implementation of ListFilesStrategy to be used when the qury contains sampling clauses.
 */
private[delta] object SamplingListFilesStrategy
    extends ListFilesStrategy
    with Logging
    with Serializable {

  override def listFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val files = listStagingAreaFiles(target, partitionFilters, dataFilters) ++
      listIndexFiles(target, partitionFilters, dataFilters)
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
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatusWithMetadata] = {
    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val snapshot = DeltaQbeastSnapshot(target.getSnapshot)
    val queryExecutor = new QueryExecutor(querySpecBuilder, snapshot)
    queryExecutor
      .execute()
      .map(block => (block.file))
      .map(file => (file.path, file))
      .toMap
      .values
      .map(IndexFiles.toFileStatusWithMetadata(target.path))
      .toSeq
  }

}
