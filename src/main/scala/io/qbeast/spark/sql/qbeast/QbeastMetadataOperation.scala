/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.model.SpaceRevision
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.delta.{
  DeltaErrors,
  MetadataMismatchErrorBuilder,
  OptimisticTransaction
}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.types.{StructField, StructType}

class QbeastMetadataOperation extends ImplicitMetadataOperation {

  /**
   * Update Metadata with Qbeast new space revision
   * @param spark SparkSession
   * @param txn write transaction
   * @param rearrangeOnly
   * @param newRevision new space revision
   */
  def updateQbeastMetadata(
      spark: SparkSession,
      txn: OptimisticTransaction,
      schema: StructType,
      isOverwriteMode: Boolean,
      rearrangeOnly: Boolean,
      newRevision: Option[SpaceRevision] = None,
      qbeastSnapshot: QbeastSnapshot): Unit = {

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

    def isNewRevision: Boolean = newRevision.isDefined
    val configuration = {
      if (isNewRevision) {
        val revisionId = newRevision.get.timestamp
        val revisions = {
          if (isOverwriteMode) Seq(newRevision)
          else qbeastSnapshot.spaceRevisions ++ Seq(newRevision)
        }
        Map(
          ("qb.lastRevisionId", revisionId.toString),
          ("qb.revisions", JsonUtils.toJson(revisions)))
      } else Map.empty[String, String]
    }

    if (txn.readVersion == -1) {
      if (dataSchema.isEmpty) {
        throw DeltaErrors.emptyDataException
      }
      recordDeltaEvent(txn.deltaLog, "delta.ddl.initializeSchema")
      // If this is the first write, configure the metadata of the table.
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException("Create a Delta table")
      }
      val description = configuration.get("comment").orNull
      val cleanedConfs = configuration.filterKeys(_ != "comment")
      txn.updateMetadata(
        Metadata(
          description = description,
          schemaString = dataSchema.json,
          partitionColumns = normalizedPartitionCols,
          configuration = cleanedConfs))
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
      if (isNewSchema) {
        errorBuilder.addSchemaMismatch(txn.metadata.schema, dataSchema, txn.metadata.id)
      }
      if (isOverwriteMode) {
        errorBuilder.addOverwriteBit()
      }
      errorBuilder.finalizeAndThrow(spark.sessionState.conf)
    } else {
      txn.updateMetadata(txn.metadata.copy(configuration = configuration))
    }
  }

  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = false
}
