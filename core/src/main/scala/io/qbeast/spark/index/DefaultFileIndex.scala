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

import io.qbeast.core.model.QbeastSnapshot
import io.qbeast.spark.index.query.QueryFiltersUtils
import io.qbeast.spark.index.strategies.DefaultListFilesStrategy
import io.qbeast.spark.index.strategies.SamplingListFilesStrategy
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

/**
 * Default implementation of the FileIndex.
 *
 * @param qbeastSnapshot
 *   the qbeast snapshot instance
 */
class DefaultFileIndex private (qbeastSnapshot: QbeastSnapshot)
    extends FileIndex
    with QueryFiltersUtils
    with Logging
    with Serializable {

  private val targetFileIndex = qbeastSnapshot.loadFileIndex()

  override def rootPaths: Seq[Path] = targetFileIndex.rootPaths

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    logFilters(partitionFilters, dataFilters)
    val strategy = if (haveQbeastWeightExpression(dataFilters)) {
      SamplingListFilesStrategy(qbeastSnapshot)
    } else {
      DefaultListFilesStrategy
    }
    strategy.listFiles(targetFileIndex, partitionFilters, dataFilters)
  }

  private def logFilters(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Unit = {
    val context = SparkSession.active.sparkContext
    val execId = context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val partitionFiltersInfo = partitionFilters.map(_.toString).mkString(" ")
    logInfo(s"DefaultFileIndex partition filters (exec id $execId): $partitionFiltersInfo")
    val dataFiltersInfo = dataFilters.map(_.toString).mkString(" ")
    logInfo(s"DefaultFileIndex data filters (exec id $execId): $dataFiltersInfo")
  }

  override def inputFiles: Array[String] = targetFileIndex.inputFiles

  override def refresh(): Unit = targetFileIndex.refresh()

  override def sizeInBytes: Long = targetFileIndex.sizeInBytes

  override def partitionSchema: StructType = targetFileIndex.partitionSchema
}

/**
 * QbeastFileIndex companion object.
 */
object DefaultFileIndex {

  /**
   * Creates a new instance from given spark session and path.
   *
   * @param qbeastSnapshot
   *   a qbeast snapshot instance
   * @return
   *   a new instance
   */
  def apply(qbeastSnapshot: QbeastSnapshot): DefaultFileIndex = {
    new DefaultFileIndex(qbeastSnapshot)
  }

}
