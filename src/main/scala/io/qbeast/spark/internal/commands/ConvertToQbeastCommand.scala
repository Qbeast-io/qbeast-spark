/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
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
import org.apache.spark.sql.delta.DeltaOperations.Convert
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
    with Logging {

  private def resolveTableFormat(spark: SparkSession): (String, TableIdentifier) =
    identifier.split("\\.") match {
      case Array(f, p) if f.nonEmpty && p.nonEmpty =>
        (f.toLowerCase(Locale.ROOT), spark.sessionState.sqlParser.parseTableIdentifier(p))
      case _ =>
        throw new RuntimeException(incorrectIdentifierFormat(identifier))
    }

  override def run(spark: SparkSession): Seq[Row] = {
    val (fileFormat, tableId) = resolveTableFormat(spark)

    val deltaLog = DeltaLog.forTable(spark, tableId.table)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
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

      // Convert delta to qbeast
      deltaLog.update()

      val txn = deltaLog.startTransaction()

      // Converting a partitioned delta table is not supported, for qbeast files
      // are not partitioned.
      val isPartitionedDelta = txn.metadata.partitionColumns.nonEmpty
      if (isPartitionedDelta) {
        throw AnalysisExceptionFactory.create(partitionedTableExceptionMsg)
      }

      val convRevision = Revision.emptyRevision(QTableID(tableId.table), cubeSize, columnsToIndex)
      val revisionID = convRevision.revisionID

      // Update revision map
      val updatedConf =
        txn.metadata.configuration
          .updated(lastRevisionID, revisionID.toString)
          .updated(s"$revision.$revisionID", mapper.writeValueAsString(convRevision))

      val newMetadata =
        txn.metadata.copy(configuration = updatedConf)

      txn.updateMetadata(newMetadata)
      txn.commit(Seq.empty, Convert(0, Seq.empty, collectStats = false, None))
    }
    Seq.empty[Row]
  }

}
