/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{IndexStatus, ReplicatedSet, RevisionChange, TableChanges, mapper}
import io.qbeast.spark.utils.MetadataConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.{
  DeltaErrors,
  MetadataMismatchErrorBuilder,
  OptimisticTransaction
}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

private[delta] class QbeastMetadataOperation extends ImplicitMetadataOperation {

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
   * @param indexStatus the index status
   * @param deltaReplicatedSet the new set of replicated cubes
   */

  private def updateQbeastReplicatedSet(
      baseConfiguration: Configuration,
      indexStatus: IndexStatus,
      deltaReplicatedSet: ReplicatedSet): Configuration = {

    val revisionID = indexStatus.revision.revisionID
    assert(baseConfiguration.contains(s"${MetadataConfig.revision}.$revisionID"))

    val oldReplicatedSet = indexStatus.replicatedSet
    val newReplicatedSet =
      oldReplicatedSet.union(deltaReplicatedSet).map(_.string)
    // Save the replicated set of cube id's as String representation

    baseConfiguration.updated(
      s"${MetadataConfig.replicatedSet}.$revisionID",
      mapper.writeValueAsString(newReplicatedSet))

  }

  private def overwriteReplicatedSet(
      baseConfiguration: Configuration,
      indexStatus: IndexStatus): Configuration = {

    val revisionID = indexStatus.revision.revisionID
    assert(baseConfiguration.contains(s"${MetadataConfig.revision}.$revisionID"))

    baseConfiguration.updated(
      s"${MetadataConfig.replicatedSet}.$revisionID",
      mapper.writeValueAsString(Set.empty))

  }

  /**
   * Update metadata with new Qbeast Revision
   * @param baseConfiguration the base configuration
   * @param revisionChange the new revision
   */
  private def updateQbeastRevision(
      baseConfiguration: Configuration,
      revisionChange: RevisionChange): Configuration = {

    val newRevision = revisionChange.newRevision
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
    def isNewSchema: Boolean = txn.metadata.schema != mergedSchema
    def isNewRevision: Boolean = tableChanges.revisionChanges.isDefined

    val baseConfiguration: Configuration =
      if (txn.readVersion == -1) Map.empty else txn.metadata.configuration
    // Qbeast configuration metadata
    val configuration = if (isNewRevision) {
      updateQbeastRevision(baseConfiguration, tableChanges.revisionChanges.get)
    } else if (isOverwriteMode) {
      // Be careful with overwrite mode when is within the same revision,
      // because it maintains old configuration if any
      overwriteReplicatedSet(baseConfiguration, tableChanges.indexChanges.supersededIndexStatus)
    } else if (isOptimizeOperation) {
      updateQbeastReplicatedSet(
        baseConfiguration,
        tableChanges.indexChanges.supersededIndexStatus,
        tableChanges.indexChanges.deltaReplicatedSet)
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
