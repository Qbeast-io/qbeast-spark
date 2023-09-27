/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model._
import io.qbeast.spark.delta.{DeltaQbeastSnapshot, SparkDeltaMetadataManager}
import io.qbeast.spark.utils.MetadataConfig.{lastRevisionID, revision}
import io.qbeast.spark.utils.QbeastExceptionMessages.{
  incorrectIdentifierFormat,
  partitionedTableExceptionMsg,
  unsupportedFormatExceptionMsg
}
import org.apache.http.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{AnalysisException, AnalysisExceptionFactory, Row, SparkSession}

import java.util.Locale

/**
 * Command to convert a parquet or a delta table into a qbeast table.
 * The command creates the an empty revision for the metadata, the qbeast options provided
 * should be those with which the user want to index the table. Partitioned tables are not
 * supported.
 * @param identifier STRING, table identifier consisting of "format.`tablePath`"
 *                   e.g. parquet.`/tmp/test/`
 * @param columnsToIndex Seq[STRING], the columns on which the index is built
 *                       e.g. Seq("col1", "col2")
 * @param cubeSize INT, the desired cube size for the index
 *                 e.g. 5000
 */
@Experimental
case class ConvertToQbeastCommand(
    identifier: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE)
    extends LeafRunnableCommand
    with Logging
    with StagingUtils {

  private def resolveTableFormat(spark: SparkSession): (String, TableIdentifier) =
    identifier.split("\\.") match {
      case Array(f, p) if f.nonEmpty && p.nonEmpty =>
        (f.toLowerCase(Locale.ROOT), spark.sessionState.sqlParser.parseTableIdentifier(p))
      case _ =>
        throw AnalysisExceptionFactory.create(incorrectIdentifierFormat(identifier))
    }

  override def run(spark: SparkSession): Seq[Row] = {
    val (fileFormat, tableId) = resolveTableFormat(spark)

    val deltaLog = DeltaLog.forTable(spark, tableId.table)
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
            spark.sql(s"CONVERT TO DELTA parquet.${tableId.quotedString}")
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
      val tableID = QTableID(tableId.table)
      val schema = deltaLog.update().schema

      SparkDeltaMetadataManager.updateMetadataWithTransaction(tableID, schema) {
        val convRevision = stagingRevision(tableID, cubeSize, columnsToIndex)
        val revisionID = convRevision.revisionID

        // Add staging revision to Revision Map, set it as the latestRevision
        Map(
          lastRevisionID -> revisionID.toString,
          s"$revision.$revisionID" -> mapper.writeValueAsString(convRevision))
      }
    }

    Seq.empty[Row]
  }

}
