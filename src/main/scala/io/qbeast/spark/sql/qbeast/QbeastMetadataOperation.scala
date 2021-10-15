/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.ReplicatedSet
import io.qbeast.spark.model.SpaceRevision
import io.qbeast.spark.sql.utils.MetadataConfig._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.delta.{
  DeltaErrors,
  MetadataMismatchErrorBuilder,
  OptimisticTransaction
}
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.types.{StructField, StructType}

class QbeastMetadataOperation extends ImplicitMetadataOperation {

  def updateQbeastReplicatedSet(
      txn: OptimisticTransaction,
      revisionTimestamp: Long,
      qbeastSnapshot: QbeastSnapshot,
      newReplicatedCubes: ReplicatedSet): Unit = {

    val revisionId = s"$metadataReplicatedSet.$revisionTimestamp"
    val oldReplicatedSet =
      qbeastSnapshot.replicatedSet(revisionTimestamp)

    val newReplicatedSet =
      oldReplicatedSet.union(newReplicatedCubes).map(_.string)
    // Save the replicated set of cube id's as String representation
    val oldConfiguration = txn.metadata.configuration

    val configuration =
      oldConfiguration.updated(revisionId, JsonUtils.toJson(newReplicatedSet))
    txn.updateMetadata(txn.metadata.copy(configuration = configuration))
  }

  /**
   * Update metadata with new Qbeast Revision
   * @param txn the transaction
   * @param data the data to write
   * @param partitionColumns partitionColumns
   * @param isOverwriteMode if it's an overwrite operation
   * @param rearrangeOnly if the operation only rearranges files
   * @param newRevision the new Qbeast revision
   * @param qbeastSnapshot the Qbeast Snapshot
   */
  def updateQbeastRevision(
      txn: OptimisticTransaction,
      data: Dataset[_],
      partitionColumns: Seq[String],
      isOverwriteMode: Boolean,
      rearrangeOnly: Boolean,
      columnsToIndex: Seq[String],
      desiredCubeSize: Int,
      newRevision: SpaceRevision,
      qbeastSnapshot: QbeastSnapshot): Unit = {

    val revisionTimestamp = newRevision.timestamp
    assert(
      !qbeastSnapshot.existsRevision(revisionTimestamp),
      s"The revision $revisionTimestamp is already present in the Metadata")

    val spark = data.sparkSession
    val schema = data.schema

    val dataSchema = StructType(schema.fields.map {
      case StructField(name, dataType, _, metadata) =>
        StructField(name, dataType, nullable = true, metadata)
    })

    val mergedSchema = if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      SchemaUtils.mergeSchemas(txn.metadata.schema, dataSchema)
    }
    val normalizedPartitionCols =
      Seq.empty

    // Merged schema will contain additional columns at the end
    def isNewSchema: Boolean = txn.metadata.schema != mergedSchema
    // Qbeast configuration metadata
    val configuration = txn.metadata.configuration
      .updated(metadataIndexedColumns, JsonUtils.toJson(columnsToIndex))
      .updated(metadataDesiredCubeSize, desiredCubeSize.toString)
      .updated(metadataLastRevisionTimestamp, revisionTimestamp.toString)
      .updated(s"$metadataRevision.$revisionTimestamp", JsonUtils.toJson(newRevision))

    if (txn.readVersion == -1) {
      super.updateMetadata(
        spark,
        txn,
        schema,
        partitionColumns,
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

  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = false
}
