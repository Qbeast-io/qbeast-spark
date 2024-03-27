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
package io.qbeast.spark.delta

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.TableChanges
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode

class MetadataWriterTest(
    tableID: QTableID,
    mode: SaveMode,
    deltaLog: DeltaLog,
    options: DeltaOptions,
    schema: StructType)
    extends DeltaMetadataWriter(tableID, mode, deltaLog, options, schema) {

  // Make updateMetadata method public for test scope
  override def updateMetadata(
      txn: OptimisticTransaction,
      tableChanges: TableChanges,
      newFiles: Seq[FileAction]): Seq[Action] = super.updateMetadata(txn, tableChanges, newFiles)

}

object MetadataWriterTest {

  def apply(
      tableID: QTableID,
      mode: SaveMode,
      deltaLog: DeltaLog,
      options: DeltaOptions,
      schema: StructType): MetadataWriterTest = {
    new MetadataWriterTest(tableID, mode, deltaLog, options, schema)
  }

}
