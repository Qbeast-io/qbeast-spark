package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.BitSet

import scala.util.Random

abstract class QbeastDataSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
    extends DataSourceScanExec {

  def createBucketedReadRDD(
      bucketSpec: BucketSpec,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions
        .flatMap { p =>
          // TODO here we could somehow filter the data
          p.files.map { f =>
            // Get metadata for each file
            val blockMetaData = fsRelation.options
            val minWeight = blockMetaData("minWeight").toInt
            val maxWeight = blockMetaData("maxWeight").toInt
            val samplePercentage = 1.0
            PartitionFileUtilFilter.getPartitionedFileFiltered(
              f,
              f.getPath,
              p.values,
              samplePercentage,
              minWeight,
              maxWeight)
          }
        }
        .groupBy { f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath).getName)
            .getOrElse(sys.error(s"Invalid bucket file ${f.filePath}"))
        }

    // TODO(SPARK-32985): Decouple bucket filter pruning and bucketed table scan
    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get

      filesGroupedToBuckets.filter { f =>
        bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    val filePartitions = optionalNumCoalescedBuckets
      .map { numCoalescedBuckets =>
        logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
        val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
        Seq.tabulate(numCoalescedBuckets) { bucketId =>
          val partitionedFiles = coalescedBuckets
            .get(bucketId)
            .map {
              _.values.flatten.toArray
            }
            .getOrElse(Array.empty)
          FilePartition(bucketId, partitionedFiles)
        }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
          FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
        }
      }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  object PartitionFileUtilFilter {

    def getPartitionedFileFiltered(
        file: FileStatus,
        filePath: Path,
        partitionValues: InternalRow,
        percentage: Double,
        min: Long,
        max: Long): PartitionedFile = {

      val offset = if (percentage < 1.0) {
        min + Random.nextInt((max - min).toInt + 1)
      } else {
        0
      }
      val hosts = getBlockHosts(getBlockLocations(file), 0, file.getLen - offset)
      PartitionedFile(partitionValues, filePath.toUri.toString, 0, file.getLen - offset, hosts)
    }

    private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
      case f: LocatedFileStatus => f.getBlockLocations
      case f => Array.empty[BlockLocation]
    }

    // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
    // pair that represents a segment of the same file, find out the block that contains the largest
    // fraction the segment, and returns location hosts of that block. If no such block can be
    // found, returns an empty array.
    private def getBlockHosts(
        blockLocations: Array[BlockLocation],
        offset: Long,
        length: Long): Array[String] = {
      val candidates = blockLocations
        .map {
          // The fragment starts from a position within this block. It handles the case where the
          // fragment is fully contained in the block.
          case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
            b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

          // The fragment ends at a position within this block
          case b
              if b.getOffset < offset + length && offset + length < b.getOffset + b.getLength =>
            b.getHosts -> (offset + length - b.getOffset)

          // The fragment fully contains this block
          case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
            b.getHosts -> b.getLength

          // The fragment doesn't intersect with this block
          case b =>
            b.getHosts -> 0L
        }
        .filter { case (hosts, size) =>
          size > 0L
        }

      if (candidates.isEmpty) {
        Array.empty[String]
      } else {
        val (hosts, _) = candidates.maxBy { case (_, size) => size }
        hosts
      }
    }

  }

}
