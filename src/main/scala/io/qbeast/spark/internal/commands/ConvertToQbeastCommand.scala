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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.http.annotation.Experimental
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
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

  private def isSupportedFormat(format: String): Boolean = {
    format == "parquet" || format == "delta"
  }

  private def convertParquetToDelta(spark: SparkSession, path: String): Unit = {
    spark.sql(s"CONVERT TO DELTA parquet.`$path`")
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

  private def createQbeastActions(
      snapshot: Snapshot,
      revision: Revision,
      path: String): IISeq[FileAction] = {
    val allFiles = snapshot.allFiles.collect()

    allFiles
      .map(addFile => {
        val metadataTag = QbeastMetadataExtractor.extractMetadataTag(addFile, revision, path)
        addFile.copy(tags = metadataTag)
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
    if (!isSupportedFormat(fileFormat)) {
      throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }
    // Make convert to delta idempotent

    // Convert parquet to delta
    if (fileFormat == "parquet") convertParquetToDelta(sparkSession, path)

    // Convert delta to qbeast
    val snapshot = DeltaLog.forTable(sparkSession, path).snapshot
    val revision = initializeRevision(path, snapshot.schema)

    SparkDeltaMetadataManager.updateWithTransaction(
      revision.tableID,
      snapshot.schema,
      append = true) {
      val tableChanges = getTableChanges(revision)
      val newFiles = createQbeastActions(snapshot, revision, path)

      (tableChanges, newFiles)
    }
    Seq.empty
  }

}

case class ColumnMinMax(minValue: Any, maxValue: Any)

object QbeastMetadataExtractor {
  private val numRecordsPattern: Regex = """"numRecords":(\d+),""".r

  def extractParquetFileCount(parquetFilePath: String): String = {
    val path = new Path(parquetFilePath)
    val file = HadoopInputFile.fromPath(path, new Configuration())
    val reader = ParquetFileReader.open(file)
    reader.getRecordCount.toString
  }

  def extractMetadataTag(
      addFile: AddFile,
      revision: Revision,
      root: String): Map[String, String] = {
    val elementCount = addFile.stats match {
      case stats: String =>
        numRecordsPattern.findFirstMatchIn(stats) match {
          case Some(matching) => matching.group(1)
          case _ => extractParquetFileCount(root + "/" + addFile.path)
        }
      case _ => extractParquetFileCount(root + "/" + addFile.path)
    }

    Map(
      TagUtils.cube -> "",
      TagUtils.minWeight -> Weight.MinValue.value.toString,
      TagUtils.maxWeight -> Weight.MaxValue.value.toString,
      TagUtils.state -> State.FLOODED,
      TagUtils.revision -> revision.revisionID.toString,
      TagUtils.elementCount -> elementCount)

  }

}
