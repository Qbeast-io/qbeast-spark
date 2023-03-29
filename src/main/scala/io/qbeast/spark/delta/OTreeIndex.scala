/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.QbeastBlock
import io.qbeast.core.model.RevisionUtils.stagingID
import io.qbeast.spark.index.query.{QueryExecutor, QuerySpecBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{
  PartitioningAwareFileIndex,
  PartitionDirectory,
  PartitionSpec
}
import org.apache.spark.sql.types.StructType

import java.net.URI
import scala.collection.mutable.LinkedHashMap

/**
 * FileIndex to prune files
 *
 * @param spark the SparkSession instance
 * @param index the Tahoe log file index
 */
case class OTreeIndex(spark: SparkSession, index: TahoeLogFileIndex)
    extends PartitioningAwareFileIndex(spark, Map.empty, None) {

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

    val qbeastFileStats = matchingBlocks(partitionFilters, dataFilters).map { qbeastBlock =>
      new FileStatus(
        /* length */ qbeastBlock.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ qbeastBlock.modificationTime,
        absolutePath(qbeastBlock.path))
    }.toArray
    val stagingStats = stagingFiles
    val fileStats = qbeastFileStats ++ stagingStats

    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), fileStats))

  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes

  override def rootPaths: Seq[Path] = index.rootPaths

  override def partitionSpec(): PartitionSpec = PartitionSpec.emptySpec

  override def partitionSchema: StructType = partitionSpec().partitionColumns

  override protected def leafFiles: LinkedHashMap[Path, FileStatus] = LinkedHashMap.empty

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = Map.empty

}

/**
 * Companion object for OTreeIndex
 * Builds an OTreeIndex instance from the path to a table
 */
object OTreeIndex {

  def apply(spark: SparkSession, path: Path): OTreeIndex = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val tahoe = TahoeLogFileIndex(spark, deltaLog, path, deltaLog.snapshot, Seq.empty, false)
    OTreeIndex(spark, tahoe)
  }

}

/**
 * File index implementation for the cases when the table is empty or does not
 * exist.
 *
 * @param spark the SparkSession instance
 */
class EmptyIndex(spark: SparkSession) extends PartitioningAwareFileIndex(spark, Map.empty, None) {

  override def rootPaths: Seq[Path] = Seq.empty

  override def refresh(): Unit = ()

  override def partitionSpec(): PartitionSpec = PartitionSpec.emptySpec

  override protected def leafFiles: LinkedHashMap[Path, FileStatus] = LinkedHashMap.empty

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = Map.empty
}
