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
 * Default implementation of the ListFilesStrategy which simply applies Delta filtering based on
 * min/max and other stats.
 */
private[delta] object DefaultListFilesStrategy extends ListFilesStrategy with Serializable {

  override def listFiles(
      target: TahoeLogFileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] =
    target.listFiles(partitionFilters, dataFilters)

}
