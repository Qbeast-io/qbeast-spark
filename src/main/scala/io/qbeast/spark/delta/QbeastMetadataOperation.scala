/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.RevisionUtils.stagingID
import io.qbeast.core.model.{ReplicatedSet, Revision, TableChanges, mapper}
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.MetadataConfig.{lastRevisionID, revision}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaMergingUtils}
import org.apache.spark.sql.delta.{
  DeltaErrors,
  MetadataMismatchErrorBuilder,
  OptimisticTransaction
}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * Qbeast metadata changes on a Delta Table.
 */
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

    // Add staging revision, if necessary. The qbeast metadata configuration
    // should always have a revision with RevisionID = stagingID.
    val stagingRevisionKey = s"$revision.$stagingID"
    val addStagingRevision =
      newRevisionID == 1 && !baseConfiguration.contains(stagingRevisionKey)
    val configuration =
      if (!addStagingRevision) baseConfiguration
      else {
        // Create staging revision with EmptyTransformers(and EmptyTransformations)
        val stagingRevision = Revision.emptyRevision(
          newRevision.tableID,
          newRevision.desiredCubeSize,
          newRevision.columnTransformers.map(_.columnName))

        // Add the staging revision to the revisionMap without overwriting
        // the latestRevisionID
        baseConfiguration
          .updated(stagingRevisionKey, mapper.writeValueAsString(stagingRevision))
      }

    // Update latest revision id and add new revision to metadata
    configuration
      .updated(lastRevisionID, newRevisionID.toString)
      .updated(s"$revision.$newRevisionID", mapper.writeValueAsString(newRevision))
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

    val mergedSchema =
      if (isOverwriteMode && canOverwriteSchema) dataSchema
      else SchemaMergingUtils.mergeSchemas(txn.metadata.schema, dataSchema)

    val normalizedPartitionCols = Seq.empty

    // Merged schema will contain additional columns at the end
    val isNewSchema: Boolean = txn.metadata.schema != mergedSchema
    val isNewRevision: Boolean = tableChanges.isNewRevision

    val latestRevision = tableChanges.updatedRevision
    val baseConfiguration: Configuration =
      if (txn.readVersion == -1) Map.empty
      else if (isOverwriteMode) overwriteQbeastConfiguration(txn.metadata.configuration)
      else txn.metadata.configuration

    // Qbeast configuration metadata
    val configuration =
      if (isNewRevision || isOverwriteMode) {
        updateQbeastRevision(baseConfiguration, latestRevision)
      } else if (isOptimizeOperation) {
        updateQbeastReplicatedSet(
          baseConfiguration,
          latestRevision,
          tableChanges.announcedOrReplicatedSet)
      } else baseConfiguration

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
      if (isOverwriteMode) errorBuilder.addOverwriteBit()
      else {
        errorBuilder.addSchemaMismatch(txn.metadata.schema, dataSchema, txn.metadata.id)
      }
      errorBuilder.finalizeAndThrow(spark.sessionState.conf)
    } else txn.updateMetadata(txn.metadata.copy(configuration = configuration))
  }

  override protected val canMergeSchema: Boolean = true
  override protected val canOverwriteSchema: Boolean = true
}
