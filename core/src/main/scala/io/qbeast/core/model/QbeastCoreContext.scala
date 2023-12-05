package io.qbeast.core.model

import io.qbeast.core.keeper.Keeper

import scala.reflect.ClassTag

/**
 * Qbeast Core main components
 * @tparam DATA
 *   type of the data
 * @tparam DataSchema
 *   type of the data schema
 * @tparam QbeastOptions
 *   type of the Qbeast options
 * @tparam FileDescriptor
 *   type of the file descriptor
 */
trait QbeastCoreContext[DATA, DataSchema, QbeastOptions, FileDescriptor] {
  def metadataManager: MetadataManager[DataSchema, FileDescriptor, QbeastOptions]
  def dataWriter: DataWriter[DATA, DataSchema, FileDescriptor]
  def indexManager: IndexManager[DATA]
  def queryManager[QUERY: ClassTag]: QueryManager[QUERY, DATA]
  def revisionBuilder: RevisionFactory[DataSchema, QbeastOptions],
  def autoIndexer: AutoIndexer[DATA]
  def keeper: Keeper

}

/**
 * RevisionFactory
 *
 * @tparam DataSchema
 *   type of the data schema
 * @tparam QbeastOptions
 *   type of the Qbeast options
 */
trait RevisionFactory[DataSchema, QbeastOptions] {

  /**
   * Create a new revision for a table with given parameters
   *
   * @param qtableID
   *   the table identifier
   * @param schema
   *   the schema
   * @param options
   *   the options
   * @return
   */
  def createNewRevision(qtableID: QTableID, schema: DataSchema, options: QbeastOptions): Revision

  /**
   * Create a new revision with given parameters from an old revision
   * @param qtableID
   *   the table identifier
   * @param schema
   *   the schema
   * @param options
   *   the options
   * @param oldRevision
   *   the old revision
   * @return
   */
  def createNextRevision(
      qtableID: QTableID,
      schema: DataSchema,
      options: QbeastOptions,
      oldRevision: RevisionID): Revision

}
