/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import io.qbeast.model.CubeId

/**
 * Index package object.
 */
package object index {

  /**
   * Normalized weight is used for estimating the cube
   * weight while indexing new data.
   */
  type NormalizedWeight = Double

  /**
   * ReplicatedSet is used to represent a set of CubeId's that had been replicated
   */
  type ReplicatedSet = Set[CubeId]

}
