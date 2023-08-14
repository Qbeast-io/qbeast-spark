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
      val (file, ranges) = QbeastFileFormat.extractRanges(fileWithRanges)
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

  private def extractRanges(fileWithRanges: PartitionedFile): (PartitionedFile, Seq[RowRange]) = {
    val path = fileWithRanges.filePath
    val indexOfSharp = path.lastIndexOf('#')
    if (indexOfSharp > 0) {
      val file = fileWithRanges.copy(filePath = path.substring(0, indexOfSharp))
      val ranges = Seq.newBuilder[RowRange]
      path.substring(indexOfSharp + 1).split(',').foreach { expression =>
        val indexOfDash = expression.indexOf('-')
        if (indexOfDash > 0) {
          val from = expression.substring(0, indexOfDash).toLong
          val to = expression.substring(indexOfDash + 1).toLong
          ranges += RowRange(from, to)
        }
      }
      return (file, ranges.result())
    }
    (fileWithRanges, Seq.empty)
  }

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
