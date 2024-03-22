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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory

/**
 * List files strategy abstraction allows to implement different approaches to compute the set of
 * files contributing to the query results.
 */
private[delta] trait ListFilesStrategy {

  /**
   * Lists the files from the given index applying the provided filters.
   *
   * @param target
   *   the target index
   * @param partitionFilters
   *   the partition filters
   * @param dataFilters
   *   the data filters
   * @return
   *   a sequence of partition directories
   */
  def listFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory]

}
