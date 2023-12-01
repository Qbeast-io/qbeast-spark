/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.Transformation

/**
 * Query space defines the domain area requested by the query.
 */
trait QuerySpace {

  /**
   * Checks if this QuerySpace contains other QuerySpace
   * @param other
   *   the other query space
   * @return
   *   true if this QuerySpace contains the other QuerySpace
   */

  def contains(other: QuerySpace): Boolean

  /**
   * Returns whether the space intersects with a given cube.
   *
   * @param cube
   *   the cube
   * @return
   *   the space intersects with the cube
   */
  def intersectsWith(cube: CubeId): Boolean
}

/**
 * Implementation of QuerySpace which represents the while domain.
 */
case class AllSpace() extends QuerySpace {

  override def intersectsWith(cube: CubeId): Boolean = true

  override def contains(other: QuerySpace): Boolean = true
}

/**
 * Implementation of QuerySpace that represents an empty domain.
 */
case class EmptySpace() extends QuerySpace {

  override def intersectsWith(cube: CubeId): Boolean = false

  // The only case in which EmptySpace contains other QuerySpace
  // is when other QuerySpace is an EmptySpace
  override def contains(other: QuerySpace): Boolean = other.isInstanceOf[EmptySpace]
}

/**
 * * Describe the query range in the area included in [originalFrom,originalTo) (inclusive,
 * exclusive).
 *
 * @param from
 *   inclusive starting range
 * @param to
 *   exclusive ending query range
 */
class QuerySpaceFromTo(private val from: Seq[Option[Double]], private val to: Seq[Option[Double]])
    extends QuerySpace {

  private def intersects(f: Double, t: Double, cube: CubeId, coordinate: Int): Boolean = {
    val cf = cube.from.coordinates(coordinate)
    val ct = cube.to.coordinates(coordinate)
    (f <= cf && cf < t) || (cf <= f && f < ct) || (f == 1.0 && ct == 1.0)
  }

  override def intersectsWith(cube: CubeId): Boolean = {
    // Use epsilon to support LessThanOrEqual: x <= t is approximated by x < t + epsilon
    val epsilon = 1.1103e-16

    from.zip(to).zipWithIndex.forall {
      case ((Some(f), Some(t)), i) => intersects(f, t + epsilon, cube, i)
      case ((None, Some(t)), i) => intersects(0.0, t + epsilon, cube, i)
      case ((Some(f), None), i) => intersects(f, 1.0, cube, i)
      case ((None, None), _) => true
    }
  }

  override def contains(other: QuerySpace): Boolean = {
    other match {
      case q: QuerySpaceFromTo =>
        q.from.zip(from).forall {
          case (Some(otherFrom), Some(f)) => otherFrom >= f
          case (Some(otherFrom), None) => otherFrom == 0.0
          case (None, Some(f)) => f == 0.0
          case _ => true
        } && q.to.zip(to).forall {
          case (Some(otherTo), Some(t)) => otherTo <= t
          case (Some(otherTo), None) => otherTo == 1.0
          case (None, Some(t)) => t == 1.0
          case _ => true
        }
      case _: AllSpace => false
      case _: EmptySpace => true
    }

  }

}

object QuerySpace {

  def apply(
      originalFrom: Seq[Option[Any]],
      originalTo: Seq[Option[Any]],
      transformations: Seq[Transformation]): QuerySpace = {
    assert(originalTo.size == originalFrom.size)
    assert(transformations.size == originalTo.size)

    val from = originalFrom.zip(transformations).map {
      case (Some(f), transformation) => Some(transformation.transform(f))
      case _ => None
    }
    val to = originalTo.zip(transformations).map {
      case (Some(t), transformation) => Some(transformation.transform(t))
      case _ => None
    }

    var isPointStringSearch = false
    var (isOverlappingSpace, isAllSpace) = (true, true)

    from.indices.foreach { i =>
      val (isOverlappingDim, isAllDim) = (from(i), to(i), transformations(i)) match {
        case (Some(f), Some(t), _: HashTransformation) if f == t =>
          isPointStringSearch = true
          (true, true)
        case (_, _, _: HashTransformation) | (None, None, _) =>
          (true, true)
        case (Some(f), Some(t), _) =>
          (f <= t && f <= 1d && t >= 0d, f <= t && f <= 0d && t >= 1d)
        case (None, Some(t), _) =>
          (t >= 0d, t >= 1d)
        case (Some(f), None, _) =>
          (f <= 1d, f <= 0d)
      }

      isOverlappingSpace &&= isOverlappingDim
      isAllSpace &&= isAllDim
    }

    if (isAllSpace && !isPointStringSearch) AllSpace()
    else if (isPointStringSearch || isOverlappingSpace) new QuerySpaceFromTo(from, to)
    else EmptySpace()
  }

}
