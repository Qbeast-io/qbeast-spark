/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.types.StructType

/**
 * Implementation of org.apache.spark.sql.connector.read.Batch.
 */
class QbeastBatch private[v2] (
    private val index: FileIndex,
    private val schema: StructType,
    private val options: Map[String, String])
    extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    Array(QbeastInputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new QbeastPartitionReaderFactory(index, schema, options)
  }

}
