/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model._
import io.qbeast.core.transform._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand.dataTypeMinMax
import io.qbeast.spark.utils.MetadataConfig
import org.apache.http.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

import java.util.Locale

@Experimental
case class ConvertToQbeastCommand(
    identifier: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE,
    partitionColumns: Option[String])
    extends LeafRunnableCommand
    with Logging
    with DeltaLogging {

  private val isPartitioned: Boolean = partitionColumns.isDefined

  /**
   * Format inference for the input table. If partition columns are provided,
   * the format is assumed to be parquet. Any unsupported format is considered
   * as parquet and is detected when trying to convert it into delta.
   */
  private def resolveTableFormat(): (String, String) =
    identifier.split("\\.") match {
      case Array(f, p) => (f.toLowerCase(Locale.ROOT), p)
      case _ => throw new RuntimeException(s"Table doesn't exists at $identifier")
    }

  /**
   * Convert the parquet table using ConvertToDeltaCommand from Delta Lake.
   * Any unsupported format will cause a SparkException error.
   */
  private def convertParquetToDelta(spark: SparkSession, path: String): Unit = {
    if (!isPartitioned) spark.sql(s"CONVERT TO DELTA parquet.`$path`")
    else {
      spark.sql(s"CONVERT TO DELTA parquet.`$path` PARTITIONED BY (${partitionColumns.get})")
    }
  }

  /**
   * Initialize Revision for table conversion.
   * The smallest RevisionID for a converted table is 0.
   * @param schema table schema
   * @return
   */
  private def initializeRevision(path: String, schema: StructType): Revision = {
    val revision =
      SparkRevisionFactory.createNewRevision(
        QTableID(path),
        schema,
        Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString))

    val transformations = revision.columnTransformers.map {
      case LinearTransformer(_, dataType: OrderedDataType) =>
        val minMax = dataTypeMinMax(dataType)
        LinearTransformation(minMax.minValue, minMax.maxValue, dataType)
      case HashTransformer(_, _) => HashTransformation()
    }.toIndexedSeq

    revision.copy(transformations = transformations)
  }

  private def isQbeastFormat(deltaLog: DeltaLog): Boolean = {
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
    val isDelta = deltaLog.tableExists

    isDelta && qbeastSnapshot.loadAllRevisions.nonEmpty
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val (fileFormat, path) = resolveTableFormat()

    val deltaLog = DeltaLog.forTable(spark, path)
    if (isQbeastFormat(deltaLog)) {
      logInfo("The table you are trying to convert is already a qbeast table")
    } else {
      fileFormat match {
        // Convert parquet to delta
        case "parquet" => convertParquetToDelta(spark, path)
        case _ =>
      }

      // Convert delta to qbeast
      val txn = deltaLog.startTransaction()
      val revision = initializeRevision(path, deltaLog.snapshot.schema)
      val revisionID = revision.revisionID
      val isOverwritingSchema = txn.metadata.partitionColumns.nonEmpty
      val updatedConf = txn.metadata.configuration
        .updated(MetadataConfig.lastRevisionID, revisionID.toString)
        .updated(
          s"${MetadataConfig.revision}.$revisionID" -> mapper.writeValueAsString(revisionID))
      val newMetadata =
        txn.metadata.copy(configuration = updatedConf, partitionColumns = Seq.empty)
      txn.updateMetadata(newMetadata)
      if (isOverwritingSchema) recordDeltaEvent(txn.deltaLog, "delta.ddl.overwriteSchema")
    }
    Seq.empty[Row]
  }

}

object ConvertToQbeastCommand {
  private val intMinMax = ColumnMinMax(Int.MaxValue, Int.MinValue)
  private val doubleMinMax = ColumnMinMax(Double.MaxValue, Double.MinValue)
  private val longMinMax = ColumnMinMax(Long.MaxValue, Long.MinValue)

  private val dataTypeMinMax = Map(
    DoubleDataType -> doubleMinMax,
    IntegerDataType -> intMinMax,
    LongDataType -> longMinMax,
    FloatDataType -> doubleMinMax,
    DecimalDataType -> doubleMinMax,
    TimestampDataType -> longMinMax,
    DateDataType -> longMinMax)

}

case class ColumnMinMax(minValue: Any, maxValue: Any)
