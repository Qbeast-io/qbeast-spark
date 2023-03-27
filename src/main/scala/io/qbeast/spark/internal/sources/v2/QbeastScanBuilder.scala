/*
 * Copyright 2023 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.types.StructType

/**
 * Implementation of [[org.apache.spark.sql.connector.read.ScanBuilder]].
 */
class QbeastScanBuilder private[v2] (
    private val index: FileIndex,
    private val schema: StructType,
    private val options: Map[String, String])
    extends ScanBuilder {

  override def build(): Scan = {
    new QbeastScan(index, schema, options)
  }

}
