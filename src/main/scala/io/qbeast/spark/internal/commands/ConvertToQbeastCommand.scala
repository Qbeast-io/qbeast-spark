/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.core.transform._
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.http.annotation.Experimental
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

@Experimental
case class ConvertToQbeastCommand(
    path: String,
    fileFormat: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE)
    extends LeafRunnableCommand {

  private val parquetFormat = "parquet"
  private val deltaFormat = "delta"
  private val acceptedFormats = Seq(parquetFormat, deltaFormat)
  private val numRecordsPattern: Regex = """"numRecords":(\d+),""".r

  private val intMinMax = ColumnMinMax(-1e8.toInt, 1e8.toInt)
  private val doubleMinMax = ColumnMinMax(-1e10, 1e10)
  private val longMinMax = ColumnMinMax(-1e15.toLong, 1e15.toLong)

  private val dataTypeMinMax = Map(
    DoubleDataType -> doubleMinMax,
    IntegerDataType -> intMinMax,
    LongDataType -> longMinMax,
    FloatDataType -> doubleMinMax,
    DecimalDataType -> doubleMinMax,
    TimestampDataType -> longMinMax,
    DateDataType -> longMinMax)

  private def convertParquetToDelta(spark: SparkSession, path: String): Unit = {
    spark.sql(s"CONVERT TO DELTA $parquetFormat.`$path`")
  }

  def initializeRevision(path: String, schema: StructType): Revision = {
    val revision =
      SparkRevisionFactory.createNewRevision(
        QTableID(path),
        schema,
        Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString))

    val transformations: IISeq[Transformation] = revision.columnTransformers.map {
      case LinearTransformer(_, dataType: OrderedDataType) =>
        val minMax = dataTypeMinMax(dataType)
        LinearTransformation(minMax.minValue, minMax.maxValue, dataType)
      case HashTransformer(_, _) => HashTransformation()
    }

    revision.copy(transformations = transformations)
  }

  private def createQbeastActions(snapshot: Snapshot, revision: Revision): IISeq[FileAction] = {
    val root = revision.createCubeIdRoot()
    val allFiles = snapshot.allFiles.collect()

    allFiles
      .map(addFile => {
        val elementCount = numRecordsPattern.findFirstMatchIn(addFile.stats) match {
          case Some(matching) => matching.group(1)
          case _ => "0"
        }
        addFile.copy(
          modificationTime = System.currentTimeMillis(),
          tags = Map(
            TagUtils.cube -> root.string,
            TagUtils.minWeight -> Weight.MinValue.value.toString,
            TagUtils.maxWeight -> Weight.MaxValue.value.toString,
            TagUtils.state -> State.FLOODED,
            TagUtils.revision -> revision.revisionID.toString,
            TagUtils.elementCount -> elementCount))
      })
      .toIndexedSeq
  }

  private def getTableChanges(revision: Revision): TableChanges = {
    val root = revision.createCubeIdRoot()

    BroadcastedTableChanges(
      isNewRevision = true,
      isOptimizeOperation = false,
      revision,
      Set.empty[CubeId],
      Set.empty[CubeId],
      SparkSession.active.sparkContext.broadcast(Map(root -> State.FLOODED)),
      SparkSession.active.sparkContext.broadcast(Map(root -> Weight.MaxValue)))
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // TODO very basic mechanism for converting to qbeast
    if (!acceptedFormats.contains(fileFormat)) {
      throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }

    // Convert parquet to delta
    if (fileFormat == parquetFormat) convertParquetToDelta(sparkSession, path)

    // Convert delta to qbeast
    val snapshot = DeltaLog.forTable(sparkSession, path).snapshot
    val revision = initializeRevision(path, snapshot.schema)

    SparkDeltaMetadataManager.updateWithTransaction(
      revision.tableID,
      snapshot.schema,
      append = true) {
      val tableChanges = getTableChanges(revision)
      val newFiles = createQbeastActions(snapshot, revision)

      (tableChanges, newFiles)
    }
    Seq.empty
  }

  case class ColumnMinMax(minValue: Any, maxValue: Any) {}
}