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
package io.qbeast.spark.internal.commands

import io.qbeast.core.model.RevisionID
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * The Optimize Table command implementation
 *
 * @param revisionID
 *   the identifier of revision to optimize
 * @param indexedTable
 *   indexed table to optimize
 */
case class OptimizeTableCommand(revisionID: RevisionID, indexedTable: IndexedTable)
    extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.optimize(revisionID)
    Seq.empty[Row]
  }

}
