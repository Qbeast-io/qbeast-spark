/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.core.transform._
import io.qbeast.spark.delta.{DeltaQbeastLog, SparkDeltaMetadataManager}
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand.{
  dataTypeMinMax,
  extractQbeastTag,
  sparkToSqlTypeNames
}
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
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE,
    partitionColumns: Seq[String] = Seq.empty)
    extends LeafRunnableCommand {

  private val isPartitioned: Boolean = partitionColumns.nonEmpty

  /**
   * Format inference for the input table. If partition columns are provided,
   * the format is assumed to be parquet. Any unsupported format is considered
   * as parquet and is detected when trying to convert it into delta.
   * @param sparkSession SparkSession to use
   * @return
   */
  private def resolveTableFormat(sparkSession: SparkSession): (String, StructType) = {
    val deltaLog = DeltaLog.forTable(sparkSession, path)
    val qbeastSnapshot = DeltaQbeastLog(deltaLog).qbeastSnapshot
    val schema = deltaLog.snapshot.schema

    val isDelta = deltaLog.tableExists
    // The first revisionID for a converted table is 0,
    // while for one that's written in the conventional fashion is 1.
    val isQbeast =
      isDelta && (qbeastSnapshot.existsRevision(0) || qbeastSnapshot.existsRevision(1))

    if (isQbeast) {
      ("qbeast", schema)
    } else if (isDelta) {
      ("delta", schema)
    } else if (isPartitioned) {
      // Partitioned parquet, table schema is required for its conversion into delta
      ("parquet", sparkSession.read.parquet(path).schema)
    } else {
      // Parquet, or any other unsupported format, schema.isEmpty but we don't need it
      ("parquet", schema)
    }
  }

  // scalastyle:off println
  def logConsole(line: String): Unit = println(line)
  // scalastyle:on println

  /**
   * Convert the parquet table using ConvertToDeltaCommand from Delta Lake.
   * Any unsupported format will cause a SparkException error.
   * @param spark SparkSession to use
   */
  private def convertParquetToDelta(spark: SparkSession, schema: StructType): Unit = {
    if (isPartitioned) {
      val colsAndTypes =
        partitionColumns.map(colName => {
          val typeName = schema(colName).dataType.typeName
          val sqlTypeName = sparkToSqlTypeNames.getOrElse(typeName, typeName)
          colName + " " + sqlTypeName
        })
      spark.sql(
        s"CONVERT TO DELTA parquet.`$path` PARTITIONED BY (${colsAndTypes.mkString(", ")})")
    } else {
      spark.sql(s"CONVERT TO DELTA parquet.`$path`")
    }
  }

  /**
   * Initialize Revision for table conversion.
   * The smallest RevisionID for a converted table is 0.
   * @param schema table schema
   * @return
   */
  private def initializeRevision(schema: StructType): Revision = {
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

  private def createQbeastActions(
      snapshot: Snapshot,
      revision: Revision,
      path: String): IISeq[FileAction] = {
    val allFiles = snapshot.allFiles.collect()

    allFiles
      .map(addFile => {
        val metadataTag = extractQbeastTag(addFile, revision, path)
        addFile.copy(tags = metadataTag)
      })
      .toIndexedSeq
  }

  private def getTableChanges(revision: Revision, sparkSession: SparkSession): TableChanges = {
    val root = revision.createCubeIdRoot()

    BroadcastedTableChanges(
      isNewRevision = true,
      isOptimizeOperation = false,
      revision,
      Set.empty[CubeId],
      Set.empty[CubeId],
      sparkSession.sparkContext.broadcast(Map(root -> State.FLOODED)),
      sparkSession.sparkContext.broadcast(Map(root -> Weight.MaxValue)))
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (fileFormat, sourceSchema) = resolveTableFormat(sparkSession)

    fileFormat match {
      // Idempotent conversion
      case "qbeast" =>
        logConsole("The table you are trying to convert is already a qbeast table")
        return Seq.empty[Row]
      // Convert parquet to delta
      case "parquet" => convertParquetToDelta(sparkSession, sourceSchema)
      // delta, do nothing
      case _ =>
    }

    // Convert delta to qbeast
    val snapshot = DeltaLog.forTable(sparkSession, path).snapshot
    val revision = initializeRevision(snapshot.schema)

    SparkDeltaMetadataManager.updateWithTransaction(
      revision.tableID,
      snapshot.schema,
      append = true) {
      val tableChanges = getTableChanges(revision, sparkSession)
      val newFiles = createQbeastActions(snapshot, revision, path)

      (tableChanges, newFiles)
    }

    Seq.empty[Row]
  }

}

object ConvertToQbeastCommand {
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

  /**
   * Map a Spark data type name to a Spark SQL type name. Currently only the following
   * are supported for conversion. Any other data type are inferred as String after
   * being used as Partition Columns. Timestamp but this is not supported as a partition
   * column by delta conversion.
   */
  private val sparkToSqlTypeNames = Map(
    "integer" -> "INT",
    "double" -> "DOUBLE",
    "long" -> "BIGINT",
    "date" -> "DATE",
    "timestamp" -> "TIMESTAMP",
    "string" -> "STRING")

  /**
   * Extract record count from a parquet file metadata.
   * @param parquetFilePath target parquet file path
   * @return
   */
  def extractParquetFileCount(parquetFilePath: String): String = {
    val path = new Path(parquetFilePath)
    val file = HadoopInputFile.fromPath(path, new Configuration())
    val reader = ParquetFileReader.open(file)
    reader.getRecordCount.toString
  }

  /**
   * Extract Qbeast metadata for an AddFile.
   * @param addFile AddFile to be converted into a qbeast block for the root
   * @param revision the conversion revision to use, revisionID = 0
   * @param tablePath path of the table
   * @return
   */
  def extractQbeastTag(
      addFile: AddFile,
      revision: Revision,
      tablePath: String): Map[String, String] = {
    val elementCount = addFile.stats match {
      case stats: String =>
        numRecordsPattern.findFirstMatchIn(stats) match {
          case Some(matching) => matching.group(1)
          // stats does not contain record count, proceed extraction using parquet metadata
          case _ => extractParquetFileCount(tablePath + "/" + addFile.path)
        }
      // AddFile entries with no 'stats' field, proceed extraction using parquet metadata
      case _ => extractParquetFileCount(tablePath + "/" + addFile.path)
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

case class ColumnMinMax(minValue: Any, maxValue: Any)
