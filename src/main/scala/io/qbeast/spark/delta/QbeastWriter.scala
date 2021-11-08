/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{IndexStatus, TableChanges}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, stateColumnName}
import io.qbeast.spark.index.writer.BlockWriter
import io.qbeast.spark.index.{OTreeAlgorithm, QbeastColumns}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame, SaveMode, SparkSession}
import org.apache.spark.util.SerializableConfiguration

/**
 * QbeastWriter is in charge of writing data to a table
 * and report the necessary log information
 *
 * @param mode SaveMode of the write
 * @param deltaLog deltaLog associated to the table
 * @param options options for write operation
 * @param partitionColumns partition columns
 * @param data data to write
 * @param revision the current table revision
 * @param qbeastSnapshot current qbeast snapshot of the table
 * @param announcedSet set of cubes announced
 * @param oTreeAlgorithm algorithm to organize data
 */
case class QbeastWriter(
    mode: SaveMode,
    deltaLog: DeltaLog,
    options: DeltaOptions,
    partitionColumns: Seq[String],
    data: DataFrame,
    indexStatus: IndexStatus,
    qbeastSnapshot: QbeastSnapshot,
    oTreeAlgorithm: OTreeAlgorithm)
    extends QbeastMetadataOperation
    with DeltaCommand {

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  /**
   * Writes data to the table
   * @param txn transaction to commit
   * @param sparkSession active SparkSession
   * @return the sequence of file actions to save in the commit log(add, remove...)
   */
  def write(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {

    import sparkSession.implicits._
    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw AnalysisExceptionFactory.create(s"Path '${deltaLog.dataPath}' already exists.'")
      } else if (mode == SaveMode.Ignore) {
        return Nil
      } else if (mode == SaveMode.Overwrite) {
        deltaLog.assertRemovable()
      }
    }
    val rearrangeOnly = options.rearrangeOnly

    val (qbeastData, tc @ TableChanges(revisionChange, _)) =
      oTreeAlgorithm.index(data, indexStatus, false) // TODO change the new ManagerAPI

    // The Metadata can be updated only once in a single transaction
    // If a new space revision is detected, we update everything in the same operation
    // If not, we delegate directly to the delta updateMetadata
    if (revisionChange.isDefined) {
      updateQbeastRevision(
        txn,
        data,
        partitionColumns,
        isOverwriteOperation,
        rearrangeOnly,
        revisionChange.get,
        qbeastSnapshot)
    } else {
      val oldQbeastMetadata = txn.metadata.configuration
      updateMetadata(
        txn,
        data,
        partitionColumns,
        oldQbeastMetadata,
        isOverwriteOperation,
        rearrangeOnly)
    }

    // Validate partition predicates
    val replaceWhere = options.replaceWhere
    val partitionFilters = if (replaceWhere.isDefined) {
      val predicates = parsePartitionPredicates(sparkSession, replaceWhere.get)
      if (mode == SaveMode.Overwrite) {
        verifyPartitionPredicates(sparkSession, txn.metadata.partitionColumns, predicates)
      }
      Some(predicates)
    } else {
      None
    }

    if (txn.readVersion < 0) {
      // Initialize the log path
      val fs = deltaLog.logPath.getFileSystem(sparkSession.sessionState.newHadoopConf)

      fs.mkdirs(deltaLog.logPath)
    }

    val newFiles = writeFiles(qbeastData, tc)
    val addFiles = newFiles.collect { case a: AddFile => a }
    val deletedFiles = (mode, partitionFilters) match {
      case (SaveMode.Overwrite, None) =>
        txn.filterFiles().map(_.remove)
      case (SaveMode.Overwrite, Some(predicates)) =>
        // Check to make sure the files we wrote out were actually valid.
        val matchingFiles = DeltaLog
          .filterFileList(txn.metadata.partitionSchema, addFiles.toDF(), predicates)
          .as[AddFile]
          .collect()
        val invalidFiles = addFiles.toSet -- matchingFiles
        if (invalidFiles.nonEmpty) {
          val badPartitions = invalidFiles
            .map(_.partitionValues)
            .map {
              _.map { case (k, v) => s"$k=$v" }.mkString("/")
            }
            .mkString(", ")
          throw AnalysisExceptionFactory.create(
            s"""Data written out does not match replaceWhere '$replaceWhere'.
               |Invalid data would be written to partitions $badPartitions.""".stripMargin)
        }

        txn.filterFiles(predicates).map(_.remove)
      case _ => Nil
    }

    if (rearrangeOnly) {
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map(_.copy(dataChange = !rearrangeOnly))
    } else {
      newFiles ++ deletedFiles
    }

  }

  /**
   * Writes qbeast indexed data into files
   * @param qbeastData the dataFrame containing data to write
   * @param tableChanges the update status of the index.
   * @return the sequence of added files to the table
   */

  def writeFiles(qbeastData: DataFrame, tableChanges: TableChanges): Seq[FileAction] = {

    val (factory: OutputWriterFactory, serConf: SerializableConfiguration) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance()
      (
        format.prepareWrite(data.sparkSession, job, Map.empty, data.schema),
        new SerializableConfiguration(job.getConfiguration))
    }
    val qbeastColumns = QbeastColumns(qbeastData)

    val blockWriter =
      BlockWriter(
        dataPath = deltaLog.dataPath.toString,
        schema = data.schema,
        schemaIndex = qbeastData.schema,
        factory = factory,
        serConf = serConf,
        qbeastColumns = qbeastColumns,
        tableChanges = tableChanges)
    qbeastData
      .repartition(col(cubeColumnName), col(stateColumnName))
      .queryExecution
      .executedPlan
      .execute
      .mapPartitions(blockWriter.writeRow)
      .collect()

  }

}
