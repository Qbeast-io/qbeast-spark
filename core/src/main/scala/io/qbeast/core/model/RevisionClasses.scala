package io.qbeast.core.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Typing
import io.qbeast.IISeq
import io.qbeast.core.transform.{Transformation, Transformer}

import scala.collection.immutable.SortedMap

object QTableID {

  @JsonCreator
  def apply(id: String): QTableID = new QTableID(id)

}

/**
 * Unique identifier class for Qbeast Table
 * @param _id the identifier in string form
 */
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

/**
 * Companion object for Revision
 */
object Revision {

  /**
   * Create a new first revision for a table
   * @param tableID the table identifier
   * @param desiredCubeSize the desired cube size
   * @param columnTransformers the column transformers
   * @return the new Revision, without any data insights
   */
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

/**
 * A revision of a QTable.
 * @param revisionID the identifier of the revision
 * @param timestamp the timestamp
 * @param tableID the table identifier
 * @param desiredCubeSize the desired cube size
 * @param columnTransformers the column transformers
 * @param transformations the space transformations
 */
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
      values.length == transformations.length && values.nonEmpty,
      s"$values $transformations")
    val vb = Vector.newBuilder[Double]
    vb.sizeHint(transformations.length)
    for (i <- transformations.indices) {
      vb += transformations(i).transform(values(i))
    }
    vb.result()
  }

}

/**
 * Container for the set of changes to a revision
 * @param timestamp the timestamp
 * @param supersededRevision the superseded revision
 * @param desiredCubeSizeChange the desired cube size option change
 * @param columnTransformersChanges the column transformers optional changes
 * @param transformationsChanges the space transformations optional changes
 */
case class RevisionChange(
    timestamp: Long,
    supersededRevision: Revision,
    desiredCubeSizeChange: Option[Int] = None,
    columnTransformersChanges: IISeq[Option[Transformer]] = Vector.empty,
    transformationsChanges: IISeq[Option[Transformation]] = Vector.empty) {

  /**
   * Creates a new revision based on the current revision and the changes
   * @return
   */
  def createNewRevision: Revision = supersededRevision match {
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

/**
 * Container for the current status of the index
 * @param revision the revision
 * @param replicatedSet the set of cubes in a replicated state
 * @param announcedSet the set of cubes in an announced state
 * @param cubesStatuses the map containing the status (maxWeight and files) of each cube
 */

case class IndexStatus(
    revision: Revision,
    replicatedSet: ReplicatedSet = Set.empty,
    announcedSet: Set[CubeId] = Set.empty,
    cubesStatuses: SortedMap[CubeId, CubeStatus] = SortedMap.empty)
    extends Serializable {

  def addAnnouncements(newAnnouncedSet: Set[CubeId]): IndexStatus =
    copy(announcedSet = announcedSet ++ newAnnouncedSet)

  def cubesToOptimize: Set[CubeId] = announcedSet.diff(replicatedSet)

  /**
   * the set of cubes that has surpass their capacity
   * @return
   */
  def overflowedSet: Set[CubeId] =
    cubesStatuses.filter(_._2.maxWeight != Weight.MaxValue).keySet

  def cubeNormalizedWeights: Map[CubeId, NormalizedWeight] =
    cubesStatuses.mapValues(_.normalizedWeight)

  def replicatedOrAnnouncedSet: Set[CubeId] = replicatedSet ++ announcedSet

}

/**
 * Container for the status information of a cube
 * @param maxWeight the max weight of the cube
 * @param normalizedWeight the normalized weight of the cube
 * @param files the files belonging to the cube
 */
case class CubeStatus(
    cubeId: CubeId,
    maxWeight: Weight,
    normalizedWeight: NormalizedWeight,
    files: IISeq[QbeastBlock])
    extends Serializable

/**
 * Companion object for the IndexStatus
 */
object IndexStatus {
  def empty(revision: Revision): IndexStatus = IndexStatus(revision)
}

trait TableChanges {
  val isNewRevision: Boolean
  val isOptimizeOperation: Boolean
  val updatedRevision: Revision
  val compressedLeaves: Set[CubeId]
  val deltaReplicatedSet: Set[CubeId]
  val announcedOrReplicatedSet: Set[CubeId]
  def cubeState(cubeId: CubeId): Option[String]
  def cubeWeights(cubeId: CubeId): Option[Weight]
}
