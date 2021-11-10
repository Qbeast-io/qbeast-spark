/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{QTableID, TableChanges}
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, SetTransaction}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisExceptionFactory, SaveMode, SparkSession}

/**
 * DeltaMetadataWriter is in charge of writing data to a table
 * and report the necessary log information
 *
 * @param mode SaveMode of the writeMetadata
 * @param deltaLog deltaLog associated to the table
 * @param options options for writeMetadata operation
 */
private[delta] case class DeltaMetadataWriter(
    qtableID: QTableID,
    mode: SaveMode,
    deltaLog: DeltaLog,
    options: DeltaOptions,
    schema: StructType)
    extends QbeastMetadataOperation
    with DeltaCommand {

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  private val sparkSession = SparkSession.active

  private def updateReplicatedFiles(
      txn: OptimisticTransaction,
      tableChanges: TableChanges): Seq[Action] = {

    val transactionID = s"qbeast.${qtableID.id}"
    val startingTnx = txn.txnVersion(transactionID)
    val newTransaction = startingTnx + 1
    val transRecord =
      SetTransaction(transactionID, newTransaction, Some(System.currentTimeMillis()))

    val deltaReplicatedSet = tableChanges.indexChanges.deltaReplicatedSet
    val cubeStrings = deltaReplicatedSet.map(_.string)
    val cubeBlocks =
      deltaLog.snapshot.allFiles
        .filter(file => cubeStrings.contains(file.tags(TagUtils.cube)))
        .collect()

    val newAddFiles = cubeBlocks.map { block =>
      block.copy(tags = block.tags.updated(TagUtils.state, State.REPLICATED))
    }
    val deleteFiles = cubeBlocks.map(_.remove)

    (newAddFiles ++ deleteFiles :+ transRecord).toSeq

  }

  /**
   * Writes metadata of the table
   * @param txn transaction to commit
   * @param tableChanges changes to apply
   * @param newFiles files to add or remove
   * @return the sequence of file actions to save in the commit log(add, remove...)
   */
  def updateMetadata(
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
        deltaLog.assertRemovable()
      }
    }
    val rearrangeOnly = options.rearrangeOnly

    def isOptimizeOperation: Boolean = tableChanges.indexChanges.deltaReplicatedSet.nonEmpty
    // The Metadata can be updated only once in a single transaction
    // If a new space revision or a new replicated set is detected,
    // we update everything in the same operation
    updateQbeastMetadata(
      txn,
      schema,
      isOverwriteOperation,
      isOptimizeOperation,
      rearrangeOnly,
      tableChanges)

    if (txn.readVersion < 0) {
      // Initialize the log path
      val fs = deltaLog.logPath.getFileSystem(sparkSession.sessionState.newHadoopConf)

      fs.mkdirs(deltaLog.logPath)
    }

    val addFiles = newFiles.collect { case a: AddFile => a }
    val deletedFiles = mode match {
      case SaveMode.Overwrite =>
        txn.filterFiles().map(_.remove)
      case _ => Nil
    }
    val optimizeActions =
      if (isOptimizeOperation) updateReplicatedFiles(txn, tableChanges) else Seq.empty

    val allFileActions = if (rearrangeOnly) {
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map(_.copy(dataChange = !rearrangeOnly))
    } else {
      newFiles ++ deletedFiles
    }

    allFileActions ++ optimizeActions

  }

}
