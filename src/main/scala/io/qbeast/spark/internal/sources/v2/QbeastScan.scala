/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.types.StructType

/**
 * Implementatio of [[org.apache.spark.sql.connector.read.Scan]].
 */
class QbeastScan private[v2] (
    private val index: FileIndex,
    private val schema: StructType,
    private val options: Map[String, String])
    extends Scan {

  override def readSchema(): StructType = {
    schema
  }

  override def toBatch(): Batch = {
    new QbeastBatch(index, schema, options)
  }

}
