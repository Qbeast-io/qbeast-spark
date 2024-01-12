/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Implementation of FileIndex to be used for empty tables.
 */
object EmptyFileIndex extends FileIndex with Serializable {

  override def rootPaths: Seq[Path] = Seq.empty

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = Seq.empty

  override def inputFiles: Array[String] = Array.empty

  override def refresh(): Unit = ()

  override def sizeInBytes: Long = 0L

  override def partitionSchema: StructType = StructType(Array.empty[StructField])

}
