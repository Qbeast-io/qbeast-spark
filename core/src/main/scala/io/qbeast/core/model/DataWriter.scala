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
package io.qbeast.core.model

import io.qbeast.IISeq

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
   * Compact the files
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
  def compact(
      tableID: QTableID,
      schema: DataSchema,
      revision: Revision,
      indexStatus: IndexStatus,
      indexFiles: Seq[IndexFile]): IISeq[FileDescriptor]

}
