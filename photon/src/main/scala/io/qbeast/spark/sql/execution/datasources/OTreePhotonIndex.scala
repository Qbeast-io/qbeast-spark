package io.qbeast.spark.sql.execution.datasources

import io.qbeast.spark.index.query._
import io.qbeast.spark.sql.execution.{QueryOperators, SampleOperator}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionSpec, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType

import java.net.URI
import scala.collection.mutable

/**
 * File Index to prune files
 * @param sparkSession the current spark session
 * @param snapshot the current QbeastSnapshot
 * @param options the options
 * @param userSpecifiedSchema the user specified schema, if any
 */
case class OTreePhotonIndex(
    sparkSession: SparkSession,
    snapshot: QbeastPhotonSnapshot,
    options: Map[String, String],
    userSpecifiedSchema: Option[StructType] = None)
    extends PartitioningAwareFileIndex(sparkSession, options, userSpecifiedSchema) {

  private var samplingOperator: Option[SampleOperator] = None

  def pushdownSample(sample: SampleOperator): Unit = {
    samplingOperator = Some(sample)
  }

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(snapshot.path, p)
    }
  }

  private def listFiles(queryOperators: QueryOperators): Seq[PartitionDirectory] = {
    // Use PhotonQueryManager to filter the files
    val querySpecBuilder = new QuerySpecBuilder(queryOperators)
    val queryExecutor = new QueryExecutor(querySpecBuilder, snapshot)
    val qbeastBlocks = queryExecutor.execute()

    // Convert QbeastBlocks into FileStatus
    val fileStats = qbeastBlocks.map { b =>
      new FileStatus(
        /* length */ b.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ b.modificationTime,
        absolutePath(b.path))
    }.toArray

    // Return a PartitionDirectory
    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), fileStats))
  }

  /**
   * List Files with pushdown filters
   * @param partitionFilters
   * @param dataFilters
   * @return
   */
  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    listFiles(QueryOperators(samplingOperator, partitionFilters ++ dataFilters))

  }

  override def allFiles(): Seq[FileStatus] = {
    listFiles(QueryOperators(None, Seq.empty)).head.files
  }

  /**
   * Return the paths of the files
   * @return
   */

  override def inputFiles: Array[String] = {
    allFiles()
      .map(f => absolutePath(f.getPath.toString).toString)
      .toArray
  }

  override def refresh(): Unit = {}

  override def sizeInBytes: Long = snapshot.allBlocks().map(_.size).sum

  override def partitionSchema: StructType = StructType(Seq.empty)

  override def partitionSpec(): PartitionSpec = PartitionSpec(partitionSchema, Seq.empty)

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    val output = mutable.LinkedHashMap[Path, FileStatus]()
    val files = allFiles()
    files.foreach(f => output += (absolutePath(f.getPath.toString) -> f))
    output

  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    val output = Map(new Path(snapshot.path) -> allFiles().toArray)
    output
  }

  override def rootPaths: Seq[Path] = Seq(new Path(snapshot.path))
}
