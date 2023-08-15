/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import io.qbeast.core.model.RowRange
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.internal.SQLConf

/**
 * FileFormat implementation based on the ParquetFileFormat.
 */
private[sources] class QbeastFileFormat extends ParquetFileFormat {

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val reader = super.buildReaderWithPartitionValues(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      options,
      hadoopConf)
    fileWithRanges: PartitionedFile => {
      val (path, ranges) = PathRangesCodec.decode(fileWithRanges.filePath)
      val file = fileWithRanges.copy(filePath = path)
      val rows = reader(file)
      QbeastFileFormat.applyRanges(rows, ranges)
    }
  }

  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = None

}

/**
 * QbeastFileFormat companion object.
 */
object QbeastFileFormat {

  private def applyRanges(
      rows: Iterator[InternalRow],
      ranges: Seq[RowRange]): Iterator[InternalRow] = {
    if (ranges.isEmpty) {
      return rows
    }
    val bufferedRows = rows.asInstanceOf[Iterator[Object]].buffered
    if (!bufferedRows.hasNext) {
      return rows
    }
    if (bufferedRows.head.isInstanceOf[ColumnarBatch]) {
      val batches = bufferedRows.asInstanceOf[Iterator[ColumnarBatch]]
      new RangedColumnarBatchIterator(batches, ranges).asInstanceOf[Iterator[InternalRow]]
    } else {
      new RangedInternalRowIterator(bufferedRows.asInstanceOf[Iterator[InternalRow]], ranges)
    }
  }

}
