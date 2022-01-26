/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.QbeastBlock
import io.qbeast.spark.index.query.{QueryExecutor, QuerySpecBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.StructType

import java.net.URI

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 */
case class OTreeIndex(index: TahoeLogFileIndex) extends FileIndex {

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
      dataFilters: Seq[Expression]): Seq[QbeastBlock] = {

    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    queryExecutor.execute()
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    val fileStats = matchingBlocks(partitionFilters, dataFilters).map { qbeastBlock =>
      new FileStatus(
        /* length */ qbeastBlock.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ qbeastBlock.modificationTime,
        absolutePath(qbeastBlock.path))
    }.toArray

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
