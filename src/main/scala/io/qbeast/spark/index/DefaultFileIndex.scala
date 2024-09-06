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

import io.qbeast.spark.index.query.QueryFiltersUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

import java.util.ServiceLoader

/**
 * Default implementation of the FileIndex.
 *
 * @param target
 *   the target file index implemented by Delta
 */
trait DefaultFileIndex extends FileIndex with QueryFiltersUtils with Logging with Serializable {

  def rootPaths: Seq[Path]

  def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory]

  def logFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Unit

  def inputFiles: Array[String]

  def refresh(): Unit

  def sizeInBytes: Long

  def partitionSchema: StructType
}

object DefaultFileIndex {

  /**
   * Creates a QbeastFileIndex instance for a given configuration.
   *
   * @param format
   *   the storage format
   * @return
   *   a MetadataManager instance
   */
  def apply(format: String, spark: SparkSession, path: Path): DefaultFileIndex = {

    val loader = ServiceLoader.load(classOf[DefaultFileIndexFactory])
    val iterator = loader.iterator()

    while (iterator.hasNext) {
      val factory = iterator.next()

      if (factory.format.equalsIgnoreCase(format)) {
        return factory.createDefaultFileIndex(spark, path)
      }
    }

    throw new IllegalArgumentException(s"No DefaultFileIndexFactory found for format: $format")

  }

}

/**
 * Factory for creating QbeastFileIndex instances. This interface should be implemented and
 * deployed by external libraries as follows: <ul> <li>Implement this interface in a class which
 * has public no-argument constructor</li> <li>Register the implementation according to
 * ServiceLoader specification</li> <li>Add the jar with the implementation to the application
 * classpath</li> </ul>
 */
trait DefaultFileIndexFactory {

  /**
   * Creates a new MetadataManager for a given configuration.
   *
   * @return
   *   a new MetadataManager
   */
  def createDefaultFileIndex(spark: SparkSession, path: Path): DefaultFileIndex

  val format: String = ???
}
