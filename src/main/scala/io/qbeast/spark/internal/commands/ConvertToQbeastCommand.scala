/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model.{BroadcastedTableChanges, CubeId, QTableID, Weight}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.http.annotation.Experimental
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
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

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // TODO very basic mechanism for converting to qbeast
    assert(acceptedFormats.contains(fileFormat), s"Format $fileFormat not supported.")

    if (fileFormat == parquetFormat) convertParquetToDelta(path)

    val options =
      Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString)

    val deltaLog = DeltaLog.forTable(sparkSession, path)
    val snapshot = deltaLog.snapshot
    val deltaFiles = snapshot.allFiles.collect()

    val revision =
      SparkRevisionFactory.createNewRevision(QTableID(path), snapshot.schema, options)

    // Assume all files are AddFiles
    val newFiles = deltaFiles
      .map(file => {
        val elementCount = numRecordsPattern.findFirstMatchIn(file.stats) match {
          case Some(matching) => matching.group(1)
          case _ => "0"
        }
        println(elementCount)
        file.copy(tags = Map(
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
