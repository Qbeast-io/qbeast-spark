/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Typing
import io.qbeast.core.transform.Transformation
import io.qbeast.core.transform.Transformer
import io.qbeast.IISeq

import scala.collection.immutable.SortedMap

object QTableID {

  @JsonCreator
  def apply(id: String): QTableID = new QTableID(id)

}

/**
 * Unique identifier class for Qbeast Table
 * @param _id
 *   the identifier in string form
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
   * @param tableID
   *   the table identifier
   * @param desiredCubeSize
   *   the desired cube size
   * @param columnTransformers
   *   the column transformers
   * @return
   *   the new Revision, without any data insights
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

  /**
   * Create a new first revision for a table with pre-loaded transformations
   * @param tableID
   *   the table identifier
   * @param desiredCubeSize
   *   the desired cube size
   * @param columnTransformers
   *   the column transformers
   * @param columnTransformations
   *   the column transformations
   * @return
   *   the new revision, with the specified transformations
   */
  def firstRevision(
      tableID: QTableID,
      desiredCubeSize: Int,
      columnTransformers: IISeq[Transformer],
      columnTransformations: IISeq[Transformation]): Revision = {
    Revision(
      0,
      System.currentTimeMillis(),
      tableID,
      desiredCubeSize,
      columnTransformers,
      columnTransformations)
  }

}

/**
 * A revision of a QTable.
 * @param revisionID
 *   the identifier of the revision
 * @param timestamp
 *   the timestamp
 * @param tableID
 *   the table identifier
 * @param desiredCubeSize
 *   the desired cube size
 * @param columnTransformers
 *   the column transformers
 * @param transformations
 *   the space transformations
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
   * Controls that this revision indexes all and only the provided columns.
   *
   * @param columnsToIndex
   *   the column names to check.
   * @return
   *   true if the revision indexes all and only the provided columns.
   */
  def matchColumns(columnsToIndex: Seq[String]): Boolean = {
    columnsToIndex.size == columnTransformers.size && columnsToIndex
      .zip(columnTransformers)
      .forall { case (columnToIndex, t) =>
        if (columnToIndex.contains(":")) {
          columnToIndex == t.spec
        } else {
          columnToIndex == t.columnName
        }
      }
  }

  /**
   * Creates a new CubeId in this revision
   * @param bytes
   *   the byte representation of the CubeId
   * @return
   *   a valid CubeID
   */
  def createCubeId(bytes: Array[Byte]): CubeId = CubeId(columnTransformers.size, bytes)

  /**
   * Creates a new CubeId in this revision
   * @param value
   *   the string representation of the CubeId
   * @return
   *   a valid CubeID
   */
  def createCubeId(value: String): CubeId = CubeId(columnTransformers.size, value)

  def createCubeIdRoot(): CubeId = CubeId.root(columnTransformers.size)

  /**
   * returns the normalized values
   * @param values
   *   row values for the indexing columns
   * @return
   *   the normalized values
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
 * @param timestamp
 *   the timestamp
 * @param supersededRevision
 *   the superseded revision
 * @param desiredCubeSizeChange
 *   the desired cube size option change
 * @param columnTransformersChanges
 *   the column transformers optional changes
 * @param transformationsChanges
 *   the space transformations optional changes
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
 *
 * @param revision
 *   the revision
 * @param cubesStatuses
 *   the map containing the status (maxWeight and files) of each cube
 */

case class IndexStatus(
    revision: Revision,
    cubesStatuses: SortedMap[CubeId, CubeStatus] = SortedMap.empty)
    extends Serializable {

  def cubeNormalizedWeights(): Map[CubeId, NormalizedWeight] =
    cubesStatuses.map { case (cubeId, status) => cubeId -> status.normalizedWeight }

  def cubeMaxWeights(): Map[CubeId, Weight] =
    cubesStatuses.map { case (cubeId, status) => cubeId -> status.maxWeight }

  def cubeElementCounts(): Map[CubeId, Long] =
    cubesStatuses.map { case (cubeId, status) => cubeId -> status.elementCount }

  /**
   * Compute domain sizes for each cube from the existing index. The domain of a given cube c is
   * computed as a fraction f of its parent domain, with f being the ratio between c's tree size
   * and its parent's subtree size.
   */
  def cubeDomains(): Map[CubeId, Double] = {
    var treeSizes = cubeElementCounts().mapValues(_.toDouble)
    val levelCubes = treeSizes.keys.groupBy(_.depth)
    val (minLevel, maxLevel) = (levelCubes.keys.min, levelCubes.keys.max)
    // cube sizes -> tree sizes
    (maxLevel until minLevel by -1) foreach { level =>
      levelCubes(level).foreach { cube =>
        val treeSize = treeSizes.getOrElse(cube, 0d)
        cube.parent match {
          case Some(parent) =>
            val parentTreeSize = treeSizes.getOrElse(parent, 0d) + treeSize
            treeSizes += parent -> parentTreeSize
          case _ => ()
        }
      }
    }
    // tree sizes -> cube domain
    var cubeDomains = Map.empty[CubeId, Double]
    (minLevel to maxLevel) foreach { level =>
      levelCubes(level).groupBy(_.parent).foreach {
        case (None, topCubes) =>
          topCubes.foreach(c => cubeDomains += (c -> treeSizes.getOrElse(c, 0d)))
        case (Some(parent), children) =>
          val parentDomain = cubeDomains.getOrElse(parent, 0d)
          val childTreeSizes = children.map(c => (c, treeSizes.getOrElse(c, 0d)))
          val subtreeSize = childTreeSizes.map(_._2).sum
          childTreeSizes.foreach { case (c, ts) =>
            val f = ts / subtreeSize
            val domain = f * parentDomain
            cubeDomains += (c -> domain)
          }
      }
    }
    cubeDomains
  }

}

object CubeStatus {

  def apply(
      cubeId: CubeId,
      maxWeight: Weight,
      normalizedWeight: NormalizedWeight,
      blocks: IISeq[Block]): CubeStatus = {
    new CubeStatus(cubeId, maxWeight, normalizedWeight, blocks.map(_.elementCount).sum)
  }

}

/**
 * Container for the status information of a cube
 *
 * @param maxWeight
 *   the max weight of the cube
 * @param normalizedWeight
 *   the normalized weight of the cube
 * @param elementCount
 *   the number of elements in the cube
 */
case class CubeStatus(
    cubeId: CubeId,
    maxWeight: Weight,
    normalizedWeight: NormalizedWeight,
    elementCount: Long)
    extends Serializable {}

/**
 * Companion object for the IndexStatus
 */
object IndexStatus {
  def empty(revision: Revision): IndexStatus = IndexStatus(revision)
}

trait TableChanges {
  val isNewRevision: Boolean
  val updatedRevision: Revision
  def cubeWeight(cubeId: CubeId): Option[Weight]
  def inputBlockElementCounts: Map[CubeId, Long]
}
