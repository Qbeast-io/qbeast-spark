/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.core.transform.Transformation

/**
 * Query space defines the domain area requested by the query.
 */
trait QuerySpace {

  /**
   * Returns whether the space intersects with a given cube.
   *
   * @param cube the cube
   * @return the space intersects with the cube
   */
  def intersectsWith(cube: CubeId): Boolean
}

/**
 * Implementation of QuerySpace which represents the while domain.
 */
case class AllSpace() extends QuerySpace {

  override def intersectsWith(cube: CubeId): Boolean = true
}

/**
 * *
 * Describe the query range in the area included in [originalFrom,originalTo)
 * (inclusive, exclusive).
 *
 * @param from inclusive starting range
 * @param to   exclusive ending query range
 */
class QuerySpaceFromTo(private val from: Seq[Option[Double]], private val to: Seq[Option[Double]])
    extends QuerySpace {

  private def intersects(f: Double, t: Double, cube: CubeId, coordinate: Int): Boolean = {
    val cf = cube.from.coordinates(coordinate)
    val ct = cube.to.coordinates(coordinate)
    (f <= cf && cf < t) || (cf <= f && f < ct) || (f == 1.0 && ct == 1.0)
  }

  override def intersectsWith(cube: CubeId): Boolean = {
    from.zip(to).zipWithIndex.forall {
      case ((Some(f), Some(t)), i) => intersects(f, t, cube, i)
      case ((None, Some(t)), i) => intersects(0.0, t, cube, i)
      case ((Some(f), None), i) => intersects(f, 1.0, cube, i)
      case ((None, None), _) => true
    }
  }

}

object QuerySpaceFromTo {

  def apply(
      originalFrom: Seq[Option[Any]],
      originalTo: Seq[Option[Any]],
      transformations: Seq[Transformation]): QuerySpaceFromTo = {

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

    new QuerySpaceFromTo(from, to)
  }

}
