package io.qbeast.model

import io.qbeast.keeper.Keeper

import scala.reflect.ClassTag

trait QbeastCoreContext[ID <: QTableID, DATA] {
  def metadataManager: MetadataManager[ID]
  def indexManager: IndexManager[DATA]
  def queryManager[QUERY: ClassTag]: QueryManager[QUERY, DATA]
  def revisionBuilder: RevisionBuilder[DATA]
  def keeper: Keeper

}

trait QTableIDProvider[T <: QTableID] {

  /**
   * Obtain a QTableID object from a given string representation
   * @param value the string representation of the QTableID
   * @return the QTableID object
   */
  def fromStringSerialization(value: String): T
}

trait RevisionBuilder[D] {
  def createNewRevision(qtableID: QTableID, data: D, options: Map[String, String]): Revision
}
