/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Query space defines the domain area requested by the query.
 */
trait QuerySpace {

  /**
   * The point with minimum coordinates.
   */
  val from: Point

  /**
   * The point with maximum coordinates.
   */
  val to: Point

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
 * @param dimensionCount the dimension count
 */
case class AllSpace(dimensionCount: Int) extends QuerySpace {

  val to: Point = Point(Vector.fill(dimensionCount)(1.0))

  val from: Point = Point(Vector.fill(dimensionCount)(0.0))

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
case class QuerySpaceFromTo(from: Point, to: Point) extends QuerySpace {

  override def intersectsWith(cube: CubeId): Boolean = {
    val ranges = from.coordinates.zip(to.coordinates)
    val cubeRanges = cube.from.coordinates.zip(cube.to.coordinates)
    ranges.zip(cubeRanges).forall { case ((f, t), (cf, ct)) =>
      (f <= cf && cf < t) || (cf <= f && f < ct)
    }
  }

}

/**
 * Companion object for QuerySpaceFromTo
 */
object QuerySpaceFromTo {

  def apply(originalFrom: Point, originalTo: Point, revision: Revision): QuerySpaceFromTo = {
    require(originalFrom <= originalTo, "from point must be < then to point")
    require(originalFrom.dimensionCount == originalTo.dimensionCount)
    val from: Point = Point(revision.transform(originalFrom.coordinates))
    val to: Point = Point(revision.transform(originalTo.coordinates))
    QuerySpaceFromTo(from, to)
  }

}

case class Space(min: Any, max: Any)
