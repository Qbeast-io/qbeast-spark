package io.qbeast.core.model

import io.qbeast.keeper.Keeper

import scala.reflect.ClassTag

trait QbeastCoreContext[DATA, DataSchema, FileAction] {
  def metadataManager: MetadataManager[DataSchema, FileAction]
  def dataWriter: DataWriter[DATA, DataSchema, FileAction]
  def indexManager: IndexManager[DATA]
  def queryManager[QUERY: ClassTag]: QueryManager[QUERY, DATA]
  def revisionBuilder: RevisionFactory[DataSchema]
  def keeper: Keeper

}

/**
 * RevisionFactory
 *
 * @tparam DataSchema
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

  def createNextRevision(
      qtableID: QTableID,
      schema: DataSchema,
      options: Map[String, String],
      oldRevision: RevisionID): Revision

}
