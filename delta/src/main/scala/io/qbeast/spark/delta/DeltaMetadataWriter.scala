/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.DeleteFile
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.PreCommitHook
import io.qbeast.core.model.PreCommitHook.PreCommitHookOutput
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastFile
import io.qbeast.core.model.QbeastHookLoader
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.TableChanges
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.QbeastExceptionMessages.partitionedTableExceptionMsg
import io.qbeast.spark.utils.TagColumns
import io.qbeast.spark.writer.StatsTracker.registerStatsTrackers
import org.apache.spark.internal.Logging
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
 * @param qbeastOptions
 *   options for writeMetadata operation
 * @param schema
 *   the schema of the table
 */
private[delta] case class DeltaMetadataWriter(
    tableID: QTableID,
    mode: SaveMode,
    deltaLog: DeltaLog,
    qbeastOptions: QbeastOptions,
    schema: StructType)
    extends DeltaMetadataOperation
    with DeltaCommand
    with Logging {

  private val options = {
    val optionsMap = qbeastOptions.toMap ++ Map("path" -> tableID.id)
    new DeltaOptions(optionsMap, SparkSession.active.sessionState.conf)
  }

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty

  private val sparkSession = SparkSession.active

  /**
   * Creates an instance of basic stats tracker on the desired transaction
   * @param txn
   *   the transaction
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

  private val preCommitHooks = new ListBuffer[PreCommitHook]()

  // Load the pre-commit hooks
  loadPreCommitHooks().foreach(registerPreCommitHooks)

  /**
   * Register a pre-commit hook
   * @param preCommitHook
   *   the hook to register
   */
  private def registerPreCommitHooks(preCommitHook: PreCommitHook): Unit = {
    if (!preCommitHooks.contains(preCommitHook)) {
      preCommitHooks.append(preCommitHook)
    }
  }

  /**
   * Load the pre-commit hooks from the options
   * @return
   *   the loaded hooks
   */
  private def loadPreCommitHooks(): Seq[PreCommitHook] =
    qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)

  /**
   * Executes all registered pre-commit hooks.
   *
   * This function iterates over all pre-commit hooks registered in the `preCommitHooks`
   * ArrayBuffer. For each hook, it calls the `run` method of the hook, passing the provided
   * actions as an argument. The `run` method of a hook is expected to return a Map[String,
   * String] which represents the output of the hook. The outputs of all hooks are combined into a
   * single Map[String, String] which is returned as the result of this function.
   *
   * It's important to note that if two or more hooks return a map with the same key, the value of
   * the key in the resulting map will be the value from the last hook that returned that key.
   * This is because the `++` operation on maps in Scala is a right-biased union operation, which
   * means that if there are duplicate keys, the value from the right operand (in this case, the
   * later hook) will overwrite the value from the left operand.
   *
   * Therefore, to avoid unexpected behavior, it's crucial to ensure that the outputs of different
   * hooks have unique keys. If there's a possibility of key overlap, the hooks should be designed
   * to handle this appropriately, for example by prefixing each key with a unique identifier for
   * the hook.
   *
   * @param actions
   *   The actions to be passed to the `run` method of each hook.
   * @return
   *   A Map[String, String] representing the combined outputs of all hooks.
   */
  private def runPreCommitHooks(actions: Seq[QbeastFile]): PreCommitHookOutput = {
    preCommitHooks.foldLeft(Map.empty[String, String]) { (acc, hook) =>
      acc ++ hook.run(actions)
    }
  }

  def writeWithTransaction(writer: => (TableChanges, Seq[IndexFile], Seq[DeleteFile])): Unit = {
    val oldTransactions = deltaLog.unsafeVolatileSnapshot.setTransactions
    // If the transaction was completed before then no operation
    for (txn <- oldTransactions; version <- options.txnVersion; appId <- options.txnAppId) {
      if (txn.appId == appId && version <= txn.version) {
        val message = s"Transaction $version from application $appId is already completed," +
          " the requested write is ignored"
        logWarning(message)
        return
      }
    }
    deltaLog.withNewTransaction(None, Some(deltaLog.update())) { txn =>
      // Register metrics to use in the Commit Info
      val statsTrackers = createStatsTrackers(txn)
      registerStatsTrackers(statsTrackers)

      // Execute write
      val (tableChanges, indexFiles, deleteFiles) = writer
      val addFiles = indexFiles.map(DeltaQbeastFileUtils.toAddFile)
      val removeFiles = deleteFiles.map(DeltaQbeastFileUtils.toRemoveFile)

      // Update Qbeast Metadata (replicated set, revision..)
      var actions = updateMetadata(txn, tableChanges, addFiles, removeFiles)
      // Set transaction identifier if specified
      for (txnVersion <- options.txnVersion; txnAppId <- options.txnAppId) {
        actions +:= SetTransaction(txnAppId, txnVersion, Some(System.currentTimeMillis()))
      }

      // Run pre-commit hooks
      val revision = tableChanges.updatedRevision
      val dimensionCount = revision.transformations.length
      val qbeastActions = actions.map(DeltaQbeastFileUtils.fromAction(dimensionCount))
      val tags = runPreCommitHooks(qbeastActions)

      // Commit the information to the DeltaLog
      val op =
        DeltaOperations.Write(mode, None, options.replaceWhere, options.userMetadata)
      txn.commit(actions = actions, op = op, tags = tags)
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
      .map(DeltaQbeastFileUtils.fromAddFile(dimensionCount))
      .flatMap(_.tryReplicateBlocks(deltaReplicatedSet))
      .map(file => {
        val addFile = DeltaQbeastFileUtils.toAddFile(file)
        addFile.copy(dataChange = false)
      })
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
   *
   * @param txn
   *   transaction to commit
   * @param tableChanges
   *   changes to apply
   * @param addFiles
   *   files to add
   * @param removeFiles
   *   files to remove
   * @return
   *   the sequence of file actions to save in the commit log(add, remove...)
   */
  protected def updateMetadata(
      txn: OptimisticTransaction,
      tableChanges: TableChanges,
      addFiles: Seq[AddFile],
      removeFiles: Seq[RemoveFile]): Seq[Action] = {

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

    val deletedFiles = mode match {
      case SaveMode.Overwrite =>
        txn.filterFiles().map(_.remove)
      case _ => removeFiles
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
