package io.qbeast.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Typing
import io.qbeast.IISeq
import io.qbeast.transform.{Transformation, Transformer}

object QTableID {

  @JsonCreator
  def apply(id: String): QTableID = new QTableID(id)

}

final class QTableID(_id: String) extends Serializable {

  def id: String = _id

  @JsonValue
  override def toString: String = _id

  def canEqual(other: Any): Boolean = other.isInstanceOf[QTableID]

  override def equals(other: Any): Boolean = other match {
    case that: QTableID =>
      toString == that.toString
    case _ => false
  }

  override def hashCode(): Int = toString.hashCode
}

object Revision {

  def firstRevision(
      tableID: QTableID,
      desiredCubeSize: Int,
      columnTransformers: IISeq[Transformer]): Revision = {
    Revision(
      0,
      System.currentTimeMillis(),
      tableID,
      desiredCubeSize,
      columnTransformers,
      Vector.empty)

  }

}

final case class Revision(
    revisionID: RevisionID,
    timestamp: Long,
    tableID: QTableID,
    desiredCubeSize: Int,
    @JsonSerialize(
      as = classOf[IISeq[Transformer]],
      typing = Typing.STATIC) columnTransformers: IISeq[Transformer],
    @JsonSerialize(
      as = classOf[IISeq[Transformation]],
      typing = Typing.STATIC) transformations: IISeq[Transformation])
    extends Serializable {
  assert(columnTransformers != null || transformations != null)

  /**
   * *
   * Controls that the this revision indexes all and only the provided columns.
   *
   * @param columnsToIndex the column names to check.
   * @return true if the revision indexes all and only the provided columns.
   */
  def matchColumns(columnsToIndex: Seq[String]): Boolean = {
    columnsToIndex.toSet == columnTransformers.map(_.columnName).toSet
  }

  /**
   * Creates a new CubeId in this revision
   * @param bytes the byte representation of the CubeId
   * @return a valid CubeID
   */
  def createCubeId(bytes: Array[Byte]): CubeId = CubeId(columnTransformers.size, bytes)

  /**
   * Creates a new CubeId in this revision
   * @param value the string representation of the CubeId
   * @return a valid CubeID
   */
  def createCubeId(value: String): CubeId = CubeId(columnTransformers.size, value)

  def createCubeIdRoot(): CubeId = CubeId.root(columnTransformers.size)

  /**
   * returns the normalized values
   * @param values
   * @return the normalized values
   */
  def transform(values: IISeq[_]): IISeq[Double] = {
    assert(
      values.length == transformations.length && values.length > 0,
      s"$values $transformations")
    val vb = Vector.newBuilder[Double]
    vb.sizeHint(transformations.length)
    for (i <- transformations.indices) {
      vb += transformations(i).transform(values(i))
    }
    vb.result()
  }

  def toJson: String = {
    mapper.writeValueAsString(this)
  }

}

case class RevisionChange(
    timestamp: Long,
    supersededRevision: Revision,
    desiredCubeSizeChange: Option[Int] = None,
    columnTransformersChanges: IISeq[Option[Transformer]] = Vector.empty,
    transformationsChanges: IISeq[Option[Transformation]] = Vector.empty) {

  def toJson: String = newRevision.toJson

  def newRevision: Revision = supersededRevision match {
    case Revision(revisionID, _, tableID, desiredCubeSize, columnTransformers, transformations) =>
      Revision(
        revisionID + 1,
        timestamp,
        tableID,
        desiredCubeSizeChange.getOrElse(desiredCubeSize),
        mergeChanges(columnTransformers, columnTransformersChanges),
        mergeChanges(transformations, transformationsChanges))
  }

  private def mergeChanges[T](oldData: IISeq[T], delta: IISeq[Option[T]]): IISeq[T] = {
    if (delta.isEmpty) {
      oldData
    } else if (oldData.isEmpty) {
      // When we are creating the space transitions
      delta.map(_.get)
    } else {
      oldData.zip(delta).map { case (oldValue, change) => change.getOrElse(oldValue) }

    }
  }

}

case class IndexStatus(
    revision: Revision,
    replicatedSet: ReplicatedSet = Set.empty,
    announcedSet: Set[CubeId] = Set.empty,
    cubeWeights: Map[CubeId, Weight] = Map.empty,
    cubeNormalizedWeights: Map[CubeId, NormalizedWeight] = Map.empty,
    overflowedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  def addAnnouncements(newAnnouncedSet: Set[CubeId]): IndexStatus =
    copy(announcedSet = announcedSet ++ newAnnouncedSet)

}

object IndexStatus {
  def empty(revision: Revision): IndexStatus = IndexStatus(revision)
}

case class IndexStatusChange(
    supersededIndexStatus: IndexStatus,
    deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
    deltaReplicatedSet: Set[CubeId] = Set.empty,
    deltaAnnouncedSet: Set[CubeId] = Set.empty)
    extends Serializable {

  def cubeWeights: Map[CubeId, Weight] = PointWeightIndexer.mergeNormalizedWeights(
    supersededIndexStatus.cubeNormalizedWeights,
    deltaNormalizedCubeWeights)

  def announcedSet: Set[CubeId] = supersededIndexStatus.announcedSet ++ deltaAnnouncedSet

  def replicatedSet: Set[CubeId] = supersededIndexStatus.replicatedSet ++ deltaReplicatedSet

  def announcedOrReplicatedSet: Set[CubeId] = announcedSet ++ replicatedSet
}

case class TableChanges(
    revisionChanges: Option[RevisionChange],
    indexChanges: IndexStatusChange) {

  def updatedRevision: Revision = revisionChanges match {
    case Some(newRev) => newRev.newRevision
    case None => indexChanges.supersededIndexStatus.revision

  }

}
