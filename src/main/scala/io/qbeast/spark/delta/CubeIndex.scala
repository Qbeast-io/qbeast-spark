/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.QueryFile
import io.qbeast.core.model.QueryFileBuilder
import io.qbeast.core.model.RevisionID
import io.qbeast.spark.internal.sources.PathRangesCodec
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StructType

import java.net.URI
import scala.collection.mutable

/**
 * FileIndex implementation to load the data of the specified cube.
 */
class CubeIndex private (
    target: TahoeLogFileIndex,
    snapshot: DeltaQbeastSnapshot,
    revisionId: RevisionID,
    cubeId: CubeId)
    extends FileIndex {

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (!snapshot.existsRevision(revisionId) || snapshot.isInitial) {
      return Seq.empty
    }
    val index = snapshot.loadIndexStatus(revisionId)
    val cubeStatus = index.cubesStatuses.get(cubeId)
    if (cubeStatus.isEmpty) {
      return Seq.empty
    }
    val builders = mutable.Map.empty[String, QueryFileBuilder]
    cubeStatus.get.files.foreach { block =>
      val builder = builders.getOrElseUpdate(block.file.path, new QueryFileBuilder(block.file))
      builder.addBlock(block)
    }
    val fileStatuses = builders.values.map(_.result()).map(queryFileToFileStatus).toSeq
    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), fileStatuses))
  }

  private def queryFileToFileStatus(queryFile: QueryFile): FileStatus = {
    val file = queryFile.file
    val path = PathRangesCodec.encode(queryFile.file.path, queryFile.ranges)
    new FileStatus(
      file.size, // length
      false, // isDir
      0, // blockReplication
      1, // blockSize
      file.modificationTime, // modificationTime
      absolutePath(path) // path
    )
  }

  private def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(target.path, p)
    }
  }

  override def rootPaths: Seq[Path] = target.rootPaths

  override def inputFiles: Array[String] = target.inputFiles

  override def refresh(): Unit = target.refresh()

  override def sizeInBytes: Long = target.sizeInBytes

  override def partitionSchema: StructType = target.partitionSchema

}

/**
 * CubesIndex companion object.
 */
object CubeIndex {

  /**
   * Creates a new instance for given Spark session, table path, revision
   * identifier and cube identifiers.
   *
   * @param spark the spark session
   * @param path the table path
   * @param snapshot the table snapshot
   * @param revisionId the revision identifier
   * @param cubeId the cube identifier
   * @return a new CubesIndex instance
   */
  def apply(
      spark: SparkSession,
      path: Path,
      snapshot: DeltaQbeastSnapshot,
      revisionId: RevisionID,
      cubeId: CubeId): CubeIndex = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val target = TahoeLogFileIndex(spark, deltaLog, path, deltaLog.snapshot, Seq.empty, false)
    new CubeIndex(target, snapshot, revisionId, cubeId)
  }

}
