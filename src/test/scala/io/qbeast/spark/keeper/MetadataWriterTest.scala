package io.qbeast.spark.keeper

import io.qbeast.core.model.{QTableID, ReplicatedSet, Revision, RevisionID, TableChanges, mapper}
import io.qbeast.spark.delta.ReplicatedFile
import io.qbeast.spark.utils.{MetadataConfig, TagColumns}
import org.apache.spark.sql.{AnalysisExceptionFactory, SaveMode, SparkSession}
import org.apache.spark.sql.delta.{
  DeltaErrors,
  DeltaLog,
  DeltaOperations,
  DeltaOptions,
  MetadataMismatchErrorBuilder,
  OptimisticTransaction
}
import org.apache.spark.sql.delta.actions.{
  Action,
  AddFile,
  FileAction,
  RemoveFile,
  SetTransaction
}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

class QbeastMetadataTest extends ImplicitMetadataOperation {

  type Configuration = Map[String, String]

  /**
   * Returns the same data type but set all nullability fields are true
   * (ArrayType.containsNull, and MapType.valueContainsNull)
   * @param dataType the data type
   * @return same data type set to null
   */
  private def asNullable(dataType: DataType): DataType = {
    dataType match {
      case array: ArrayType => array.copy(containsNull = true)
      case map: MapType => map.copy(valueContainsNull = true)
      case other => other
    }
  }

  /**
   * Updates Delta Metadata Configuration with new replicated set
   * for given revision
   * @param baseConfiguration Delta Metadata Configuration
   * @param revision the new revision to persist
   * @param deltaReplicatedSet the new set of replicated cubes
   */

  private def updateQbeastReplicatedSet(
      baseConfiguration: Configuration,
      revision: Revision,
      deltaReplicatedSet: ReplicatedSet): Configuration = {

    val revisionID = revision.revisionID
    assert(baseConfiguration.contains(s"${MetadataConfig.revision}.$revisionID"))

    val newReplicatedSet = deltaReplicatedSet.map(_.string)
    // Save the replicated set of cube id's as String representation

    baseConfiguration.updated(
      s"${MetadataConfig.replicatedSet}.$revisionID",
      mapper.writeValueAsString(newReplicatedSet))

  }

  private def overwriteQbeastConfiguration(baseConfiguration: Configuration): Configuration = {
    val revisionKeys = baseConfiguration.keys.filter(_.startsWith(MetadataConfig.revision))
    val replicatedSetKeys = {
      baseConfiguration.keys.filter(_.startsWith(MetadataConfig.replicatedSet))
    }
    val other = baseConfiguration.keys.filter(_ == MetadataConfig.lastRevisionID)
    val qbeastKeys = revisionKeys ++ replicatedSetKeys ++ other
    baseConfiguration -- qbeastKeys
  }

  /**
   * Update metadata with new Qbeast Revision
   * @param baseConfiguration the base configuration
   * @param newRevision the new revision
   */
  private def updateQbeastRevision(
      baseConfiguration: Configuration,
      newRevision: Revision): Configuration = {

    val newRevisionID = newRevision.revisionID

    // Qbeast configuration metadata
    baseConfiguration
      .updated(MetadataConfig.lastRevisionID, newRevisionID.toString)
      .updated(
        s"${MetadataConfig.revision}.$newRevisionID",
        mapper.writeValueAsString(newRevision))

  }

  def updateQbeastMetadata(
      txn: OptimisticTransaction,
      schema: StructType,
      isOverwriteMode: Boolean,
      isOptimizeOperation: Boolean,
      rearrangeOnly: Boolean,
      tableChanges: TableChanges): Unit = {

    val spark = SparkSession.active

    val dataSchema = StructType(schema.fields.map(field =>
      field.copy(nullable = true, dataType = asNullable(field.dataType))))

    val mergedSchema = if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      SchemaUtils.mergeSchemas(txn.metadata.schema, dataSchema)
    }
    val normalizedPartitionCols =
      Seq.empty

    // Merged schema will contain additional columns at the end
    val isNewSchema: Boolean = txn.metadata.schema != mergedSchema
    val isNewRevision: Boolean = tableChanges.isNewRevision

    val latestRevision = tableChanges.updatedRevision
    val baseConfiguration: Configuration =
      if (txn.readVersion == -1) Map.empty
      else if (isOverwriteMode) overwriteQbeastConfiguration(txn.metadata.configuration)
      else txn.metadata.configuration
    // Qbeast configuration metadata
    val configuration = if (isNewRevision || isOverwriteMode) {
      updateQbeastRevision(baseConfiguration, latestRevision)
    } else if (isOptimizeOperation) {
      updateQbeastReplicatedSet(
        baseConfiguration,
        latestRevision,
        tableChanges.announcedOrReplicatedSet)
    } else { baseConfiguration }

    if (txn.readVersion == -1) {
      super.updateMetadata(
        spark,
        txn,
        schema,
        Seq.empty,
        configuration,
        isOverwriteMode,
        rearrangeOnly)
    } else if (isOverwriteMode && canOverwriteSchema && isNewSchema) {
      // Can define new partitioning in overwrite mode
      val newMetadata = txn.metadata.copy(
        schemaString = dataSchema.json,
        partitionColumns = normalizedPartitionCols,
        configuration = configuration)
      recordDeltaEvent(txn.deltaLog, "delta.ddl.overwriteSchema")
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException(
          "Overwrite the Delta table schema or " +
            "change the partition schema")
      }
      txn.updateMetadata(newMetadata)
    } else if (isNewSchema && canMergeSchema) {
      logInfo(s"New merged schema: ${mergedSchema.treeString}")
      recordDeltaEvent(txn.deltaLog, "delta.ddl.mergeSchema")
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException("Change the Delta table schema")
      }
      txn.updateMetadata(
        txn.metadata.copy(schemaString = mergedSchema.json, configuration = configuration))
    } else if (isNewSchema) {
      recordDeltaEvent(txn.deltaLog, "delta.schemaValidation.failure")
      val errorBuilder = new MetadataMismatchErrorBuilder
      if (isOverwriteMode) {
        errorBuilder.addOverwriteBit()
      } else {
        errorBuilder.addSchemaMismatch(txn.metadata.schema, dataSchema, txn.metadata.id)
      }
      errorBuilder.finalizeAndThrow(spark.sessionState.conf)
    } else {
      txn.updateMetadata(txn.metadata.copy(configuration = configuration))
    }
  }

  override protected val canMergeSchema: Boolean = true
  override protected val canOverwriteSchema: Boolean = true
}

case class MetadataWriterTest(
    tableID: QTableID,
    mode: SaveMode,
    deltaLog: DeltaLog,
    options: DeltaOptions,
    schema: StructType)
    extends QbeastMetadataTest
    with DeltaCommand {

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  private val sparkSession = SparkSession.active

  private val deltaOperation = {
    DeltaOperations.Write(mode, None, options.replaceWhere, options.userMetadata)
  }

  def writeWithTransaction(writer: => (TableChanges, Seq[FileAction])): Unit = {
    deltaLog.withNewTransaction { txn =>
      val (changes, newFiles) = writer
      val finalActions = updateMetadata(txn, changes, newFiles)
      txn.commit(finalActions, deltaOperation)
    }
  }

  private def updateReplicatedFiles(tableChanges: TableChanges): Seq[Action] = {

    val revision = tableChanges.updatedRevision
    val deltaReplicatedSet = tableChanges.deltaReplicatedSet

    val cubeStrings = deltaReplicatedSet.map(_.string)
    val cubeBlocks =
      deltaLog.snapshot.allFiles
        .where(TagColumns.revision === lit(revision.revisionID.toString) &&
          TagColumns.cube.isInCollection(cubeStrings))
        .collect()

    val newReplicatedFiles = cubeBlocks.map(ReplicatedFile(_))
    val deleteFiles = cubeBlocks.map(_.remove)

    deleteFiles ++ newReplicatedFiles

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

    val isOptimizeOperation: Boolean = tableChanges.isOptimizeOperation

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
      case _ => newFiles.collect { case r: RemoveFile => r }
    }

    val allFileActions = if (rearrangeOnly) {
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map(_.copy(dataChange = !rearrangeOnly))
    } else {
      newFiles ++ deletedFiles
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
