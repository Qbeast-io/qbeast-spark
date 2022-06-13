package io.qbeast.core.model

import io.qbeast.core.transform.{Transformation, Transformer}

case class EmptyTableChanges(
    isNewRevision: Boolean = false,
    isOptimizeOperation: Boolean = false,
    updatedRevision: Revision = Revision(
      0,
      System.currentTimeMillis(),
      QTableID("empty"),
      0,
      Seq.empty[Transformer].toIndexedSeq,
      Seq.empty[Transformation].toIndexedSeq),
    deltaReplicatedSet: Set[CubeId] = Set.empty,
    announcedOrReplicatedSet: Set[CubeId] = Set.empty,
    cubeStates: Map[CubeId, String] = Map.empty,
    cubeWeights: Map[CubeId, Weight] = Map.empty)
    extends TableChanges {
  override def cubeState(cubeId: CubeId): Option[String] = None

  override def cubeWeights(cubeId: CubeId): Option[Weight] = None
}
