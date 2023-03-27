/*
 * Copyright 2023 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/**
 * Implementation of [[org.apache.spark.sql.connector.read.PartitionReaderFactory]].
 */
class QbeastPartitionReaderFactory private[v2] (
    private val index: FileIndex,
    private val schema: StructType,
    private val options: Map[String, String])
    extends PartitionReaderFactory {

  private lazy val readPartitionedFile: PartitionedFile => Iterator[InternalRow] = {
    val format = new ParquetFileFormat()
    val spark = SparkSession.active
    val partitionSchema = StructType(Seq.empty)
    val config = spark.sessionState.newHadoopConfWithOptions(options)
    format.buildReaderWithPartitionValues(
      spark,
      schema,
      partitionSchema,
      schema,
      Seq.empty,
      options,
      config)
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    require(partition == QbeastInputPartition)
    val directories = index.listFiles(Seq.empty, Seq.empty).iterator
    val iterator = directories.flatMap(readDirectory)
    new QbeastPartitionReader(iterator)
  }

  private def readDirectory(directory: PartitionDirectory): Iterator[InternalRow] = {
    val PartitionDirectory(partitionValues, files) = directory
    files.iterator.map(fileStatusToPartitionedFile(partitionValues)).flatMap(readPartitionedFile)
  }

  private def fileStatusToPartitionedFile(partitionValues: InternalRow)(
      file: FileStatus): PartitionedFile =
    PartitionedFileUtil.getPartitionedFile(file, file.getPath(), partitionValues)

}
