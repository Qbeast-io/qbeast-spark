/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

/**
 * Implementation of [[org.apache.spark.sql.connector.read.PartitionReader]].
 */
class QbeastPartitionReader private[v2] (private val iterator: Iterator[InternalRow])
    extends PartitionReader[InternalRow] {

  override def close(): Unit = {
    // Nothing to do
  }

  override def next(): Boolean = {
    iterator.hasNext
  }

  override def get(): InternalRow = {
    iterator.next()
  }

}
