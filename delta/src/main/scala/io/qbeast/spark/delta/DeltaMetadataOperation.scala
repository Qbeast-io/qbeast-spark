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

import io.qbeast.spark.metadata.MetadataOperation
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.MetadataMismatchErrorBuilder
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession

/**
 * Qbeast metadata changes on a Delta Table.
 */
private[delta] trait DeltaMetadataOperation
    extends MetadataOperation
    with ImplicitMetadataOperation {

  /**
   * Update Qbeast Metadata
   * @param txn
   *   OptimisticTransaction
   * @param schema
   *   the schema of the table
   * @param isOverwriteMode
   *   whether the write mode is overwrite
   * @param rearrangeOnly
   *   whether the operation is only to rearrange the table
   * @param configuration
   *   the changes in the table
   * @param hasRevisionUpdate
   *   the Qbeast options to update
   */
  def updateTableMetadata(
      txn: OptimisticTransaction,
      schema: StructType,
      isOverwriteMode: Boolean,
      rearrangeOnly: Boolean,
      configuration: Configuration,
      hasRevisionUpdate: Boolean): Unit = {

    // TODO This whole class is starting to be messy, and contains a lot of IF then ELSE
    // TODO We should refactor QbeastMetadataOperation to make it more readable and usable
    // TODO Ideally, we should delegate this process to the underlying format

    val spark = SparkSession.active

    // Update the schema
    val dataSchema = StructType(schema.fields.map(field =>
      field.copy(nullable = true, dataType = asNullable(field.dataType))))
    val mergedSchema =
      if (isOverwriteMode && canOverwriteSchema) dataSchema
      else SchemaMergingUtils.mergeSchemas(txn.metadata.schema, dataSchema)
    // Merged schema will contain additional columns at the end
    val isNewSchema: Boolean = txn.metadata.schema != mergedSchema

    // Update table metadata
    if (!txn.deltaLog.tableExists) {
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
        partitionColumns = Seq.empty,
        configuration = configuration)
      recordDeltaEvent(txn.deltaLog, "delta.ddl.overwriteSchema")
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException(
          "Overwrite the Delta table schema or change the partition schema")
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
    } else if (hasRevisionUpdate) {
      // Aside from rewrites and schema changes, we will update the metadata only
      // when there's a Revision update. Metadata entries interrupt concurrent writes.
      txn.updateMetadata(txn.metadata.copy(configuration = configuration))
    }
  }

  override protected val canMergeSchema: Boolean
  override protected val canOverwriteSchema: Boolean
}
