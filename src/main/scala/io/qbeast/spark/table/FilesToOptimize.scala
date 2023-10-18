/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

/**
 * Index files to optimize.
 */
sealed abstract class FilesToOptimize extends Serializable

/**
 * FilesToOptimize companion object.
 */
object FilesToOptimize {

  /**
   * All the index files should optimized.
   */
  final case object All extends FilesToOptimize

  /**
   * None of the index files should be optimized.
   */
  final case object None extends FilesToOptimize

  /**
   * Some index files should be optimized.
   *
   * @param files the files to optimize
   */
  final case class Some(files: Seq[String]) extends FilesToOptimize
}
