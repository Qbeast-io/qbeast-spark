/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index

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
