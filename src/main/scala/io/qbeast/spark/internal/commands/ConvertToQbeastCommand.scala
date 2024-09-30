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

import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.utils.MetadataConfig.lastRevisionId
import io.qbeast.spark.utils.MetadataConfig.revision
import io.qbeast.spark.utils.QbeastExceptionMessages.incorrectIdentifierFormat
import io.qbeast.spark.utils.QbeastExceptionMessages.partitionedTableExceptionMsg
import io.qbeast.spark.utils.QbeastExceptionMessages.unsupportedFormatExceptionMsg
import org.apache.hadoop.fs.Path
import org.apache.http.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Command to convert a parquet or a delta table into a qbeast table. The command creates an empty
 * revision for the metadata, the qbeast options provided should be those with which the user want
 * to index the table. Partitioned tables are not supported.
 * @param identifier
 *   STRING, table identifier consisting of "format.`tablePath`" e.g. parquet.`/tmp/test/`
 * @param columnsToIndex
 *   Seq[STRING], the columns on which the index is built e.g. Seq("col1", "col2")
 * @param cubeSize
 *   INT, the desired cube size for the index e.g. 5000
 */
@Experimental
case class ConvertToQbeastCommand(
    identifier: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE,
    tableOptions: Map[String, String] = Map.empty)
    extends LeafRunnableCommand
    with Logging
    with StagingUtils {

  private def resolveTableFormat(spark: SparkSession): (String, TableIdentifier) = {

    val tableIdentifier =
      try {
        spark.sessionState.sqlParser.parseTableIdentifier(identifier)
      } catch {
        case _: AnalysisException =>
          throw AnalysisExceptionFactory.create(incorrectIdentifierFormat(identifier))
      }
    // If the table is a path table, it is a parquet or delta/qbeast table
    val provider = tableIdentifier.database.getOrElse("")
    val isPathTable = new Path(tableIdentifier.table).isAbsolute
    val isCorrectFormat = provider == "parquet" || provider == "delta" || provider == "qbeast"

    if (isPathTable && isCorrectFormat) (provider, tableIdentifier)
    else if (!isCorrectFormat)
      throw AnalysisExceptionFactory.create(unsupportedFormatExceptionMsg(provider))
    else
      throw AnalysisExceptionFactory.create(incorrectIdentifierFormat(identifier))
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val (fileFormat, tableIdentifier) = resolveTableFormat(spark)

    val deltaLog = DeltaLog.forTable(spark, tableIdentifier.table)
    val unsafeVolatileSnapshot = deltaLog.update()
    val qbeastSnapshot = DeltaQbeastSnapshot(unsafeVolatileSnapshot)
    val isQbeast = qbeastSnapshot.loadAllRevisions.nonEmpty

    if (isQbeast) {
      logInfo("The table you are trying to convert is already a qbeast table")
    } else {
      fileFormat match {
        // Convert parquet to delta
        case "parquet" =>
          try {
            spark.sql(s"CONVERT TO DELTA parquet.${tableIdentifier.quotedString}")
          } catch {
            case e: AnalysisException =>
              val deltaMsg = e.getMessage()
              throw AnalysisExceptionFactory.create(
                partitionedTableExceptionMsg +
                  s"Failed to convert the parquet table into delta: $deltaMsg")
          }
        case "delta" =>
        case _ => throw AnalysisExceptionFactory.create(unsupportedFormatExceptionMsg(fileFormat))
      }

      // Convert delta to qbeast through metadata modification
      val tableId = QTableId(tableIdentifier.table)
      val schema = deltaLog.update().schema

      SparkDeltaMetadataManager.updateMetadataWithTransaction(tableId, schema) {
        val convRevision = stagingRevision(tableId, cubeSize, columnsToIndex)
        val revisionId = convRevision.revisionId

        // Add staging revision to Revision Map, set it as the latestRevision
        Map(
          lastRevisionId -> revisionId.toString,
          s"$revision.$revisionId" -> mapper.writeValueAsString(convRevision)) ++ tableOptions
      }
    }

    Seq.empty[Row]
  }

}
