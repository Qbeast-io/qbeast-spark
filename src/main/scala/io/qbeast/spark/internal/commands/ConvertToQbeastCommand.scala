/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.utils.MetadataConfig.{lastRevisionID, revision}
import org.apache.http.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations.Convert
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{AnalysisExceptionFactory, Row, SparkSession}

import java.util.Locale

/**
 * Command to convert a parquet or a delta table, partitioned or not, into a qbeast table.
 * The command creates the an empty revision for the metadata, the qbeast options provided
 * should be those with which the user want to index the table. When the source table is
 * partitioned, the partitionColumns should be provided in the form of "columnName DataType",
 * e.g. "col1 STRING".
 * @param identifier STRING, table identifier consisting of "format.`tablePath`"
 *                   e.g. parquet.`/tmp/test/`
 * @param columnsToIndex Seq[STRING], the columns on which the index is built
 *                       e.g. Seq("col1", "col2")
 * @param cubeSize INT, the desired cube size for the index
 *                 e.g. 5000
 * @param partitionColumns STRING, the columns with which the source table is partitioned
 *                         "col1 STRING, col2 INT"
 */
@Experimental
case class ConvertToQbeastCommand(
    identifier: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE,
    partitionColumns: Option[String] = None)
    extends LeafRunnableCommand
    with Logging
    with DeltaLogging {

  private val isPartitioned: Boolean = partitionColumns.isDefined

  private def resolveTableFormat(spark: SparkSession): (String, TableIdentifier) =
    identifier.split("\\.") match {
      case Array(f, p) =>
        (f.toLowerCase(Locale.ROOT), spark.sessionState.sqlParser.parseTableIdentifier(p))
      case _ => throw new RuntimeException(s"Table doesn't exists at $identifier")
    }

  /**
   * Convert the parquet table using ConvertToDeltaCommand from Delta Lake.
   */
  private def convertParquetToDelta(spark: SparkSession, path: String): Unit = {
    val conversionCommand =
      if (!isPartitioned) s"CONVERT TO DELTA parquet.$path"
      else s"CONVERT TO DELTA parquet.$path PARTITIONED BY (${partitionColumns.get})"

    spark.sql(conversionCommand)
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
        case "parquet" => convertParquetToDelta(spark, tableId.quotedString)
        case "delta" =>
        case _ => throw AnalysisExceptionFactory.create(s"Unsupported file format: $fileFormat")
      }

      // Convert delta to qbeast
      deltaLog.update()

      val txn = deltaLog.startTransaction()

      val convRevision = Revision.emptyRevision(QTableID(tableId.table), cubeSize, columnsToIndex)
      val revisionID = convRevision.revisionID

      // If the table has partition columns, its conversion to qbeast will
      // remove them by overwriting the schema
      val isOverwritingSchema = txn.metadata.partitionColumns.nonEmpty

      // Update revision map
      val updatedConf =
        txn.metadata.configuration
          .updated(lastRevisionID, revisionID.toString)
          .updated(s"$revision.$revisionID", mapper.writeValueAsString(convRevision))

      val newMetadata =
        txn.metadata.copy(configuration = updatedConf, partitionColumns = Seq.empty)

      txn.updateMetadata(newMetadata)
      if (isOverwritingSchema) recordDeltaEvent(txn.deltaLog, "delta.ddl.overwriteSchema")
      txn.commit(Seq.empty, Convert(0, Seq.empty, collectStats = false, None))
    }
    Seq.empty[Row]
  }

}

case class ColumnMinMax(minValue: Any, maxValue: Any)
