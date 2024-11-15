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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

/**
 * Data Writer template
 */
trait DataWriter {

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
      schema: StructType,
      data: DataFrame,
      tableChanges: TableChanges,
      commitTime: String): IISeq[IndexFile]

}
