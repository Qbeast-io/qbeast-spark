package io.qbeast.spark.delta

import io.qbeast.core.model._
import org.apache.spark.sql.delta._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.actions.{Action, FileAction}

class MetadataWriterTest(
    tableID: QTableID,
    mode: SaveMode,
    deltaLog: DeltaLog,
    options: DeltaOptions,
    schema: StructType)
    extends DeltaMetadataWriter(tableID, mode, deltaLog, options, schema) {

  // Make updateMetadata method public for test scope
  override def updateMetadata(
      txn: OptimisticTransaction,
      tableChanges: TableChanges,
      newFiles: Seq[FileAction]): Seq[Action] = super.updateMetadata(txn, tableChanges, newFiles)

}

object MetadataWriterTest {

  def apply(
      tableID: QTableID,
      mode: SaveMode,
      deltaLog: DeltaLog,
      options: DeltaOptions,
      schema: StructType): MetadataWriterTest = {
    new MetadataWriterTest(tableID, mode, deltaLog, options, schema)
  }

}
