/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model.Revision.stagingID
import io.qbeast.core.model._
import io.qbeast.core.transform._
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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisExceptionFactory, Row, SparkSession}

import java.util.Locale

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

  /**
   * Initialize Revision for table conversion. The RevisionID for a converted table is 0.
   * EmptyTransformers and EmptyTransformations are used. This Revision should always be
   * superseded.
   * @param schema table schema
   * @return
   */
  private def emptyRevision(path: String, schema: StructType): Revision = {
    val transformers = columnsToIndex.map(s => EmptyTransformer(s)).toIndexedSeq
    val transformations = transformers.map(_.makeTransformation(r => r))

    Revision(
      0,
      System.currentTimeMillis(),
      QTableID(path),
      cubeSize,
      transformers,
      transformations)
  }

  private def isQbeastFormat(spark: SparkSession, path: String): Boolean = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    qbeastSnapshot.existsRevision(stagingID) || qbeastSnapshot.loadAllRevisions.nonEmpty
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val (fileFormat, tableId) = resolveTableFormat(spark)

    if (isQbeastFormat(spark, tableId.table)) {
      logInfo("The table you are trying to convert is already a qbeast table")
    } else {
      fileFormat match {
        // Convert parquet to delta
        case "parquet" => convertParquetToDelta(spark, tableId.quotedString)
        case "delta" =>
        case _ => throw AnalysisExceptionFactory.create(s"Unsupported file format: $fileFormat")
      }

      // Convert delta to qbeast
      val deltaLog = DeltaLog.forTable(spark, tableId.table)

      val txn = deltaLog.startTransaction()

      val convRevision = emptyRevision(tableId.table, deltaLog.snapshot.schema)
      val revisionID = convRevision.revisionID

      // If the table has partition columns, its conversion to qbeast will
      // remove them by overwriting the schema
      val isOverwritingSchema = txn.metadata.partitionColumns.nonEmpty

      // Add revision map as a metadata entry
      val updatedConf = txn.metadata.configuration
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
