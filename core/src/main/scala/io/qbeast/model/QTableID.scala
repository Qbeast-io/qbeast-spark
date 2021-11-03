package io.qbeast.model

import io.qbeast.IISeq
import io.qbeast.keeper.Keeper
import io.qbeast.transform.Transformer

trait QTableID {
  def serializeToString: String
  def prettyString: String
}

// TODO this RevisionContainer would replace Revision case class
trait RevisionContainer {
  def revisionID: RevisionID
  def timestamp: Long
  def qtable: QTableID
  def desiredCubeSize: Int
  def columnTransformers: IISeq[Transformer]
  def transformations: IISeq[Transformation]

  def createCubeId(bytes: Array[Byte]): CubeId = CubeId(columnTransformers.size, bytes)

}

trait RevisionChange {
  def supersededRevision: RevisionContainer
  def desiredCubeSizeChange: Option[Int]
  def columnTransformersChanges: IISeq[Option[Transformer]]
  def transformationsChanges: IISeq[Option[Transformation]]
}

trait IndexStatus {

  def revisionSpace: RevisionContainer
  def replicatedSet: Set[CubeId]
  def cubeWeights: Map[CubeId, Weight]

}

trait IndexStatusChange {
  def supersededIndexStatus: IndexStatus

  def deltaReplicatedSet: Set[CubeId]
  def deltaCubeWeights: Map[CubeId, Weight]
}

case class TableChanges(
    revisionChanges: Option[RevisionChange],
    indexChanges: Option[IndexStatusChange])

trait QTableIDProvider {

  /**
   * Obtain a QTableID object from a given string representation
   * @param value the string representation of the QTableID
   * @return the QTableID object
   */
  def fromStringSerialization(value: String): QTableID
}

trait MetadataManager {

  /**
   * Obtain the latest IndexStatus for a given QTableID
   * @param qtable the QTableID
   * @return the latest IndexStatus for qtable
   */
  def loadLatestIndexStatus(qtable: QTableID): IndexStatus

  /**
   * Obtain all IndexStatuses for a given QTableID
   * @param qtable the QTableID
   * @return an immutable Seq of IndexStatus for qtable
   */
  def loadAllIndexStatus(qtable: QTableID): IISeq[IndexStatus]

  /**
   * Obtain all Revisions for a given QTableID
   * @param qtable the QTableID
   * @return an immutable Seq of Revision for qtable
   */
  def loadAllRevisions(qtable: QTableID): IISeq[Revision]

  /**
   * Obtain the IndexStatus for a given RevisionID
   * @param revisionID the RevisionID
   * @return the IndexStatus for revisionID
   */
  def loadRevisionStatus(revisionID: RevisionID): IndexStatus

  /**
   * Loads the most updated revision at a given timestamp
   * @param timestamp the timestamp in Long format
   * @return the latest Revision at a concrete timestamp
   */
  def loadRevisionAt(timestamp: Long): Revision

  /**
   * Loads the most updated IndexStatus at a given timestamp
   * @param timestamp the timestamp in Long format
   * @return the latest IndexStatus at a concrete timestamp
   */
  def loadRevisionStatusAt(timestamp: Long): IndexStatus

  /**
   * Update the Revision with the given RevisionChanges
   * @param revisionChange the collection of RevisionChanges
   */
  def updateRevision(revisionChange: RevisionChange): Unit

  /**
   * Update the IndexStatus with the given IndexStatusChanges
   * @param indexStatusChange the collection of IndexStatusChanges
   */
  def updateIndexStatus(indexStatusChange: IndexStatusChange): Unit

  /**
   * Update the Table with the given TableChanges
   * @param tableChanges the collection of TableChanges
   */
  def updateTable(tableChanges: TableChanges): Unit

  /**
   * Perform an Update operation by using transcation control provided by Delta
   * @param code the code to be executed
   */
  def updateWithTransaction(code: _ => TableChanges): Unit

}

trait IndexManager[T] {

  def index(data: T, indexStatus: IndexStatus): TableChanges

  def optimize(data: T, indexStatus: IndexStatus): TableChanges
}

trait QueryManager[Q, T] {

  def query(query: Q, indexStatus: IndexStatus): T

}

trait QbeastCoreContext {
  def qtableIDProvider: QTableIDProvider
  def metadataManager: MetadataManager
  def indexManager[T]: IndexManager[T]
  def queryManager[Q, T]: QueryManager[Q, T]
  def keeper: Keeper

}
