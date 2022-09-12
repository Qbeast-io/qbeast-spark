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
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

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

  def convertParquetToDelta(path: String): Unit = {}

  def initializeRevision(path: String, schema: StructType): Revision = {
    val revision =
      SparkRevisionFactory.createNewRevision(
        QTableID(path),
        schema,
        Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString))

    val transformations: IISeq[Transformation] = revision.columnTransformers.map {
      case LinearTransformer(_, dataType: OrderedDataType) =>
        LinearTransformation(Int.MinValue, Int.MaxValue, dataType)
      case HashTransformer(_, _) => HashTransformation()
    }

    revision.copy(transformations = transformations)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // TODO very basic mechanism for converting to qbeast
    assert(acceptedFormats.contains(fileFormat), s"Format $fileFormat not supported.")

    if (fileFormat == parquetFormat) convertParquetToDelta(path)

    val deltaLog = DeltaLog.forTable(sparkSession, path)
    val snapshot = deltaLog.snapshot
    val deltaFiles = snapshot.allFiles.collect()

    val revision = initializeRevision(path, snapshot.schema)

    val newFiles = deltaFiles
      .map(file => {
        val elementCount = numRecordsPattern.findFirstMatchIn(file.stats) match {
          case Some(matching) => matching.group(1)
          case _ => "0"
        }
        file.copy(
          dataChange = true,
          tags = Map(
            TagUtils.cube -> "",
            TagUtils.minWeight -> Weight.MinValue.value.toString,
            TagUtils.maxWeight -> Weight.MaxValue.value.toString,
            TagUtils.state -> State.FLOODED,
            TagUtils.revision -> revision.revisionID.toString,
            TagUtils.elementCount -> elementCount))
      })
      .toIndexedSeq

    SparkDeltaMetadataManager.updateWithTransaction(
      revision.tableID,
      snapshot.schema,
      append = false) {
      val tableChanges = BroadcastedTableChanges(
        isNewRevision = false,
        isOptimizeOperation = false,
        revision,
        Set.empty[CubeId],
        Set.empty[CubeId],
        SparkSession.active.sparkContext.broadcast(
          Map(revision.createCubeIdRoot() -> State.FLOODED)),
        SparkSession.active.sparkContext.broadcast(
          Map(revision.createCubeIdRoot -> Weight.MaxValue)))

      (tableChanges, newFiles)
    }
//      throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    Seq.empty
  }

}
