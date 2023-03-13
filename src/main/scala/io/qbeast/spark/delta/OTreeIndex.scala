/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.{QbeastBlock, Weight}
import io.qbeast.core.model.RevisionUtils.stagingID
import io.qbeast.spark.index.query.{QueryExecutor, QuerySpecBuilder}
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
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

  private def deltaMatchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatus] = {
    index
      .matchingFiles(partitionFilters, dataFilters)
      .filter(a =>
        a.tags != null && a.tags.contains(TagUtils.state) && a.tags(
          TagUtils.state) == State.FLOODED)
      .map { a: AddFile =>
        new FileStatus(
          /* length */ a.size,
          /* isDir */ false,
          /* blockReplication */ 0,
          /* blockSize */ 1,
          /* modificationTime */ a.modificationTime,
          absolutePath(a.path))
      }
  }

  /**
   * Collect Staging AddFiles from _delta_log and convert them into FileStatuses.
   * The output is merged with those built from QbeastBlocks.
   * @return
   */
  private def stagingFiles: Seq[FileStatus] = {
    qbeastSnapshot.loadRevisionBlocks(stagingID).collect().map { a: AddFile =>
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

    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)

    val fileStats = if (querySpecBuilder.isSampling || !DeltaReadingUtils.useStats) {
      // If the query involves sampling predicate or the Delta Format does not have stats
      // (so it does not apply data skipping on files)
      // We filter the blocks with Qbeast
      val qbeastFiles = matchingBlocks(partitionFilters, dataFilters).map { qbeastBlock =>
        new FileStatus(
          /* length */ qbeastBlock.size,
          /* isDir */ false,
          /* blockReplication */ 0,
          /* blockSize */ 1,
          /* modificationTime */ qbeastBlock.modificationTime,
          absolutePath(qbeastBlock.path))
      }

      qbeastFiles ++ stagingFiles
    } else deltaMatchingFiles(partitionFilters, dataFilters)

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

object OTreeIndex {

  def apply(spark: SparkSession, path: Path): OTreeIndex = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val tahoe = TahoeLogFileIndex(spark, deltaLog, path, deltaLog.snapshot, Seq.empty, false)
    OTreeIndex(tahoe)
  }

}
