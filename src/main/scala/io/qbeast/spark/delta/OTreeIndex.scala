/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.QbeastFile
import io.qbeast.spark.index.query.{QueryExecutor, QuerySpecBuilder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 */
case class OTreeIndex(index: TahoeLogFileIndex)
    extends TahoeFileIndex(index.spark, index.deltaLog, index.path) {

  /**
   * Snapshot to analyze
   * @return the snapshot
   */
  protected def snapshot: Snapshot = index.getSnapshot

  private def qbeastSnapshot = DeltaQbeastSnapshot(snapshot)

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val tahoeMatchingFiles = index.matchingFiles(partitionFilters, dataFilters)
    val previouslyMatchedFiles =
      tahoeMatchingFiles.map(addFile => QbeastFile(addFile.path, addFile.tags))

    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    val qbeastMatchingFiles = queryExecutor.execute(previouslyMatchedFiles)

    tahoeMatchingFiles.filter { a => qbeastMatchingFiles.exists(_.path == a.path) }
  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes
}
