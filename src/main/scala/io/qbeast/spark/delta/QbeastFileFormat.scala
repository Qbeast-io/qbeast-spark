/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.spark.delta

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Implementation of the FileFormat to be used for reading the data.
 */
class QbeastFileFormat extends ParquetFileFormat {

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    // It is assumed that the partition file path encodes
    val reader = super.buildReaderWithPartitionValues(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      options,
      hadoopConf)
    file: PartitionedFile => {
      decodeFileFromTo(file) match {
        case Some((decodedFile, from, to)) => reader(decodedFile).take(to).drop(from)
        case None => reader(file)
      }
    }
  }

  private def decodeFileFromTo(file: PartitionedFile): Option[(PartitionedFile, Int, Int)] = {
    val PartitionedFile(
      partitionValues,
      filePath,
      start,
      length,
      locations,
      modificationTime,
      fileSize) = file
    // The filePath should be <decodedFilePath>#<from>,<to>, if not then return None
    val sharpIndex = filePath.lastIndexOf('#')
    if (sharpIndex < 0) {
      return None
    }
    val commaIndex = filePath.indexOf(',', sharpIndex + 1)
    if (commaIndex < 0) {
      return None
    }
    val decodedFilePath = filePath.substring(0, sharpIndex)
    val from =
      try {
        filePath.substring(sharpIndex + 1, commaIndex).toInt
      } catch {
        case e: NumberFormatException => return None
      }
    val to =
      try {
        filePath.substring(commaIndex + 1).toInt
      } catch {
        case e: NumberFormatException => return None
      }
    val decodedFile = PartitionedFile(
      partitionValues,
      decodedFilePath,
      start,
      length,
      locations,
      modificationTime,
      fileSize)
    Some((decodedFile, from, to))
  }

}
