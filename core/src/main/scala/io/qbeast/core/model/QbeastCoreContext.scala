package io.qbeast.core.model

import io.qbeast.core.keeper.Keeper

/**
 * Qbeast Core main components
 * @tparam DATA type of the data
 * @tparam QUERY the type of query
 * @tparam DataSchema type of the data schema
 * @tparam FileDescriptor type of the file descriptor
 */
trait QbeastCoreContext[DATA, QUERY, DataSchema, FileDescriptor] {
  def metadataManager: MetadataManager[DataSchema, FileDescriptor]
  def dataWriter: DataWriter[DATA, DataSchema, FileDescriptor]
  def indexManager: IndexManager[DATA]
  def queryManager: QueryManager[QUERY]
  def revisionBuilder: RevisionFactory[DataSchema]
  def keeper: Keeper

}

/**
 * RevisionFactory
 *
 * @tparam DataSchema type of the data schema
 */
trait RevisionFactory[DataSchema] {

  /**
   * Create a new revision for a table with given parameters
   *
   * @param qtableID      the table identifier
   * @param schema        the schema
   * @param options       the options
   * @return
   */
  def createNewRevision(
      qtableID: QTableID,
      schema: DataSchema,
      options: Map[String, String]): Revision

  /**
   * Create a new revision with given parameters from an old revision
   * @param qtableID the table identifier
   * @param schema the schema
   * @param options the options
   * @param oldRevision the old revision
   * @return
   */
  def createNextRevision(
      qtableID: QTableID,
      schema: DataSchema,
      options: Map[String, String],
      oldRevision: RevisionID): Revision

}
