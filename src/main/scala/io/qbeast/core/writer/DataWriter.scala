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
package io.qbeast.core.writer

import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.model.TableChanges
import io.qbeast.IISeq
import org.apache.spark.sql.Dataset

import java.util.ServiceConfigurationError
import java.util.ServiceLoader

/**
 * Data Writer template
 * @tparam DATA
 *   type of data
 * @tparam DataSchema
 *   type of the data schema
 * @tparam FileDescriptor
 *   type of file descriptor
 */
trait DataWriter[DATA, DataSchema, FileDescriptor] {

  /**
   * Write the index data to the files
   * @param tableID
   *   the table identifier
   * @param schema
   *   the schema of the data
   * @param data
   *   the data to write
   * @param tableChanges
   *   the changes of the index
   * @return
   *   the sequence of files written
   */
  def write(
      tableID: QTableID,
      schema: DataSchema,
      data: DATA,
      tableChanges: TableChanges): IISeq[FileDescriptor]

  /**
   * Optimize the files
   * @param tableID
   *   the table identifier
   * @param schema
   *   the schema of the data
   * @param revision
   *   the revision of the index
   * @param indexStatus
   *   the current index status
   * @param indexFiles
   *   the index files to compact
   * @return
   *   the sequence of files written and deleted
   */
  def optimize(
      tableID: QTableID,
      schema: DataSchema,
      revision: Revision,
      indexStatus: IndexStatus,
      indexFiles: Dataset[IndexFile]): IISeq[FileDescriptor]

}

object DataWriter {

  /**
   * Creates a DataWriter instance for a given configuration.
   *
   * @param format
   *   storege format
   * @return
   *   a DataWriter instance
   */
  def apply[DATA, DataSchema, FileDescriptor](
      format: String): DataWriter[DATA, DataSchema, FileDescriptor] = {

    val loader = ServiceLoader.load(classOf[DataWriterFactory[DATA, DataSchema, FileDescriptor]])
    val iterator = loader.iterator()

    while (iterator.hasNext) {
      val factory = iterator.next()

      if (factory.format.equalsIgnoreCase(format)) {
        return factory.createDataWriter()
      }
    }

    throw new IllegalArgumentException(s"No DataWriterFactory found for format: $format")

  }

}

/**
 * Factory for creating DataWriter instances. This interface should be implemented and deployed by
 * external libraries as follows: <ul> <li>Implement this interface in a class which has public
 * no-argument constructor</li> <li>Register the implementation according to ServiceLoader
 * specification</li> <li>Add the jar with the implementation to the application classpath</li>
 * </ul>
 */
trait DataWriterFactory[DATA, DataSchema, FileDescriptor] {

  /**
   * Creates a new DataWriter for a given configuration.
   *
   * @param config
   *   the configuration
   * @return
   *   a new DataWriter
   */
  def createDataWriter(): DataWriter[DATA, DataSchema, FileDescriptor]

  val format: String = ???
}
