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
import org.apache.spark.sql.{SparkSession}

import java.net.URI
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile}

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

  /**
   * Returns the matching blocks for the query
   * @param partitionFilters the query partition filters
   * @param dataFilters the query data filters
   * @return a sequence with the QbeastBlocks to read
   */
  protected def matchingBlocks(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[QbeastBlock] = {

    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    queryExecutor.execute()
  }

  protected def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    // Filter qbeast blocks
    val matchingBlocksPath = matchingBlocks(partitionFilters, dataFilters).map(_.path)
    // Apply data skipping strategy
    val deltaFilesForScan = snapshot
      .filesForScan(projection = Nil, partitionFilters ++ dataFilters)
      .files
    // Join
    deltaFilesForScan.filter(f => matchingBlocksPath.contains(f.path))
  }

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    val fileStats = matchingFiles(partitionFilters, dataFilters).map { file =>
      new FileStatus(
        /* length */ file.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ file.modificationTime,
        absolutePath(file.path))
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

/**
 * Object OTreeIndex to create a new OTreeIndex
 * @param sparkSession the spark session
 * @param path the path to the delta log
 * @return the OTreeIndex
 */
object OTreeIndex {

  def apply(spark: SparkSession, path: Path): OTreeIndex = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val tahoe = TahoeLogFileIndex(spark, deltaLog, path, deltaLog.snapshot, Seq.empty, false)
    OTreeIndex(tahoe)
  }

}
