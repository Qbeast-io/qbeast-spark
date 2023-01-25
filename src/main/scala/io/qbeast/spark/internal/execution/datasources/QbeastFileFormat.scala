package io.qbeast.spark.internal.execution.datasources

import io.qbeast.core.model.Weight
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.util.Random

/**
 * Qbeast File Format that helps skipping rows by their random weight
 * @param blockMap
 */
class QbeastFileFormat(blockMap: Map[String, Weight]) extends ParquetFileFormat {

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {

    val sampleUpperBound = options.get("sampleUpperBound") match {
      case Some(value) => Weight(value.toDouble)
      case None => Weight.MaxValue
    }

    val iterators = super
      .buildReaderWithPartitionValues(
        sparkSession,
        dataSchema,
        partitionSchema,
        requiredSchema,
        filters,
        options,
        hadoopConf)

    (pf: PartitionedFile) => {
      val maxW = blockMap(pf.filePath)
      val random = Random
      val rowIterator = iterators(pf).asInstanceOf[ColumnarBatch].rowIterator()
      val rows = Vector.newBuilder[InternalRow]
      while (rowIterator.hasNext) {
        val nx = rowIterator.next()
        if (random.nextInt(maxW.value) < sampleUpperBound.value) rows += nx
      }
      rows.result().toIterator

    }

  }

}
