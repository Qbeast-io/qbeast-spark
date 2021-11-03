package io.qbeast.model

import io.qbeast.IISeq
import io.qbeast.keeper.Keeper
import io.qbeast.transform.Transformer

trait QTableID {
  def serializeToString: String
  def prettyString: String
}

// TODO this RevisionContainer would replace Revision case class
trait RevisionContainter {
  def revisionID: RevisionID
  def timestamp: Long
  def qtable: QTableID
  def desiredCubeSize: Int
  def columnTransformers: IISeq[Transformer]
  def transformations: IISeq[Transformation]

  def createCubeId(bytes: Array[Byte]): CubeId = CubeId(columnTransformers.size, bytes)

}

trait RevisionChange {
  def supersededRevision: RevisionContainter
  def desiredCubeSizeChange: Option[Int]
  def columnTransformersChanges: IISeq[Option[Transformer]]
  def transformationsChanges: IISeq[Option[Transformation]]
}

trait IndexStatus {

  def revisionSpace: RevisionContainter
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
  def fromStringSerialization(value: String): QTableID
}

trait MetadataManager {
  def loadLatestIndexStatus(qtable: QTableID): IndexStatus
  def loadAllIndexStatus(qtable: QTableID): IISeq[IndexStatus]
  def loadAllRevisions(qtable: QTableID): IISeq[Revision]
  def loadRevisionStatus(revisionID: RevisionID): IndexStatus

  /**
   * Loads the most updated revision at a given timestamp
   * @param timestamp
   * @return the latest Revision at a concrete timestamp
   */
  def loadRevisionAt(timestamp: Long): Revision
  def loadRevisionStatusAt(timestamp: Long): IndexStatus

  def updateRevision(revisionChange: RevisionChange): Unit

  def updateIndexStatus(indexStatusChange: IndexStatusChange): Unit

  def updateTable(tableChanges: TableChanges): Unit

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
