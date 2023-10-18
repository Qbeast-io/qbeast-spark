/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.core.model.RevisionID

/**
 * Cubes to optimize.
 */
sealed abstract class CubesToOptimize extends Serializable

/**
 * CubesToOptimize companion object.
 */
object CubesToOptimize {

  /**
   * All the cubes should be optimized.
   */
  final case object All extends CubesToOptimize

  /**
   * None of the cubes should be optimized.
   */
  final case object None extends CubesToOptimize

  /**
   * Some cubes should be optimized.
   *
   * @param cubes the identifiers of the cubes to optimize grouped by revision
   */
  final case class Some(cubes: Map[RevisionID, Seq[String]]) extends Serializable
}
