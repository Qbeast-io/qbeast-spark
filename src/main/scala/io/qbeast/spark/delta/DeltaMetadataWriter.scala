/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.TableChanges
import io.qbeast.spark.delta.writer.StatsTracker.registerStatsTrackers
import io.qbeast.spark.utils.QbeastExceptionMessages.partitionedTableExceptionMsg
import io.qbeast.spark.utils.TagColumns
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ListBuffer

/**
 * DeltaMetadataWriter is in charge of writing data to a table and report the necessary log
 * information
 *
 * @param tableID
 *   the table identifier
 * @param mode
 *   SaveMode of the writeMetadata
 * @param deltaLog
 *   deltaLog associated to the table
 * @param options
 *   options for writeMetadata operation
 * @param schema
 *   the schema of the table
 */
private[delta] case class DeltaMetadataWriter(
    tableID: QTableID,
    mode: SaveMode,
    deltaLog: DeltaLog,
    options: DeltaOptions,
    schema: StructType)
    extends QbeastMetadataOperation
    with DeltaCommand {

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  private val sparkSession = SparkSession.active

  private val deltaOperation = {
    DeltaOperations.Write(mode, None, options.replaceWhere, options.userMetadata)
  }

  /**
   * Creates an instance of basic stats tracker on the desired transaction
   * @param txn
   * @return
   */
  private def createStatsTrackers(txn: OptimisticTransaction): Seq[WriteJobStatsTracker] = {
    val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()
    // Create basic stats trackers to add metrics on the Write Operation
    val hadoopConf = sparkSession.sessionState.newHadoopConf() // TODO check conf
    val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
      new SerializableConfiguration(hadoopConf),
      BasicWriteJobStatsTracker.metrics)
    txn.registerSQLMetrics(sparkSession, basicWriteJobStatsTracker.driverSideMetrics)
    statsTrackers.append(basicWriteJobStatsTracker)
    statsTrackers
  }

  def writeWithTransaction(writer: => (TableChanges, Seq[FileAction])): Unit = {
    val oldTransactions = deltaLog.unsafeVolatileSnapshot.setTransactions
    // If the transaction was completed before then no operation
    for (txn <- oldTransactions; version <- options.txnVersion; appId <- options.txnAppId) {
      if (txn.appId == appId && txn.version == version) {
        return
      }
    }
    deltaLog.withNewTransaction(None, Some(deltaLog.update())) { txn =>
      // Register metrics to use in the Commit Info
      val statsTrackers = createStatsTrackers(txn)
      registerStatsTrackers(statsTrackers)
      // Execute write
      val (changes, newFiles) = writer
      // Update Qbeast Metadata (replicated set, revision..)
      var actions = updateMetadata(txn, changes, newFiles)
      // Set transaction identifier if specified
      for (txnVersion <- options.txnVersion; txnAppId <- options.txnAppId) {
        actions +:= SetTransaction(txnAppId, txnVersion, Some(System.currentTimeMillis()))
      }
      // Commit the information to the DeltaLog
      txn.commit(actions, deltaOperation)
    }
  }

  def updateMetadataWithTransaction(update: => Configuration): Unit = {
    deltaLog.withNewTransaction(None, Some(deltaLog.update())) { txn =>
      if (txn.metadata.partitionColumns.nonEmpty) {
        throw AnalysisExceptionFactory.create(partitionedTableExceptionMsg)
      }

      val config = update
      val updatedConfig = config.foldLeft(txn.metadata.configuration) { case (accConf, (k, v)) =>
        accConf.updated(k, v)
      }
      val updatedMetadata = txn.metadata.copy(configuration = updatedConfig)

      val op = DeltaOperations.SetTableProperties(config)
      txn.updateMetadata(updatedMetadata)
      txn.commit(Seq.empty, op)
    }
  }

  private def updateReplicatedFiles(tableChanges: TableChanges): Seq[Action] = {
    val revision = tableChanges.updatedRevision
    val dimensionCount = revision.transformations.length
    val deltaReplicatedSet = tableChanges.deltaReplicatedSet
    deltaLog
      .update()
      .allFiles
      .where(TagColumns.revision === lit(revision.revisionID.toString))
      .collect()
      .map(IndexFiles.fromAddFile(dimensionCount))
      .flatMap(_.tryReplicateBlocks(deltaReplicatedSet))
      .map(IndexFiles.toAddFile(false))
      .toSeq
  }

  private def updateTransactionVersion(
      txn: OptimisticTransaction,
      revisionID: RevisionID): SetTransaction = {
    val transactionID = s"qbeast.${tableID.id}.$revisionID"
    val startingTnx = txn.txnVersion(transactionID)
    val newTransaction = startingTnx + 1

    SetTransaction(transactionID, newTransaction, Some(System.currentTimeMillis()))
  }

  /**
   * Writes metadata of the table
   * @param txn
   *   transaction to commit
   * @param tableChanges
   *   changes to apply
   * @param newFiles
   *   files to add or remove
   * @return
   *   the sequence of file actions to save in the commit log(add, remove...)
   */
  protected def updateMetadata(
      txn: OptimisticTransaction,
      tableChanges: TableChanges,
      newFiles: Seq[FileAction]): Seq[Action] = {

    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw AnalysisExceptionFactory.create(s"Path '${deltaLog.dataPath}' already exists.'")
      } else if (mode == SaveMode.Ignore) {
        return Nil
      } else if (mode == SaveMode.Overwrite) {
        DeltaLog.assertRemovable(txn.snapshot)
      }
    }
    val rearrangeOnly = options.rearrangeOnly

    val isOptimizeOperation: Boolean = tableChanges.isOptimizeOperation

    // The Metadata can be updated only once in a single transaction
    // If a new space revision or a new replicated set is detected,
    // we update everything in the same operation
    updateQbeastMetadata(txn, schema, isOverwriteOperation, rearrangeOnly, tableChanges)

    if (txn.readVersion < 0) {
      // Initialize the log path
      val fs = deltaLog.logPath.getFileSystem(sparkSession.sessionState.newHadoopConf)

      fs.mkdirs(deltaLog.logPath)
    }

    val addFiles = newFiles.collect { case a: AddFile => a }
    val deletedFiles = mode match {
      case SaveMode.Overwrite =>
        txn.filterFiles().map(_.remove)
      case _ => newFiles.collect { case r: RemoveFile => r }
    }

    val allFileActions = if (rearrangeOnly) {
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map(_.copy(dataChange = !rearrangeOnly))
    } else {
      addFiles ++ deletedFiles
    }

    if (isOptimizeOperation) {
      val revisionID = tableChanges.updatedRevision.revisionID
      val transactionRecord =
        updateTransactionVersion(txn, revisionID)
      val replicatedFiles = updateReplicatedFiles(tableChanges)
      allFileActions ++ replicatedFiles ++ Seq(transactionRecord)
    } else allFileActions

  }

}
