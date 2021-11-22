package io.qbeast.model

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

trait RevisionFactory[DataSchema] {

  def createNewRevision(
      qtableID: QTableID,
      schema: DataSchema,
      options: Map[String, String]): Revision

}
