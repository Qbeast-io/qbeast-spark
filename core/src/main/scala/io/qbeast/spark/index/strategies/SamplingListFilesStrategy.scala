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
package io.qbeast.spark.index.strategies

import io.qbeast.core.model.QbeastSnapshot
import io.qbeast.spark.index.query.QueryExecutor
import io.qbeast.spark.index.query.QuerySpecBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.SparkSession

/**
 * Implementation of ListFilesStrategy to be used when the query contains sampling clauses.
 */
case class SamplingListFilesStrategy(snapshot: QbeastSnapshot)
    extends ListFilesStrategy
    with Logging
    with Serializable {

  override def listFiles(
      target: FileIndex,
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    val files = snapshot.listUnindexedFiles(target, partitionFilters, dataFilters) ++
      listIndexFiles(partitionFilters, dataFilters)
    logFilteredFiles(files)
    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), files))
  }

  private def listIndexFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[FileStatusWithMetadata] = {

    val querySpecBuilder = new QuerySpecBuilder(dataFilters ++ partitionFilters)
    val queryExecutor = new QueryExecutor(querySpecBuilder, snapshot)
    queryExecutor.execute(snapshot.basePath)
  }

  private def logFilteredFiles(files: Seq[FileStatusWithMetadata]): Unit = logDebug {
    {
      val context = SparkSession.active.sparkContext
      val execId = context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      val count = snapshot.numOfFiles
      val filteredCount = count - files.length
      val filteredPercent = (filteredCount.toDouble / count) * 100.0
      val info = f"$filteredCount of $count ($filteredPercent%.2f%%)"
      s"Sampling filtered files (exec id $execId): $info"
    }
  }

}

object SamplingListFilesStrategy {

  def apply(snapshot: QbeastSnapshot): SamplingListFilesStrategy =
    new SamplingListFilesStrategy(snapshot)

}
