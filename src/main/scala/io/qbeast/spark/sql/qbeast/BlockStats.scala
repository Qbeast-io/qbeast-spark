/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.Weight

/**
 * Stats of a data block.
 *
 * @param cube the cube
 * @param maxWeight the maximum weight
 * @param minWeight the minimum weight
 * @param state the cube state
 * @param rowCount the number of rows
 */
case class BlockStats protected (
    cube: String,
    maxWeight: Weight,
    minWeight: Weight,
    state: String,
    rowCount: Long) {

  def update(): BlockStats =
    this.copy(rowCount = rowCount + 1)

  override def toString: String =
    s"BlocksStats($cube,${maxWeight},${minWeight},$state,$rowCount)"

}

/**
 * BlockStats companion object.
 */
object BlockStats {

  /**
   * Use this constructor to build the first BlockStat
   * @param cube the block's cubeId
   * @param state the status of the block
   * @param maxWeight the maxWeight of the block
   * @return a new empty instance of BlockStats
   */
  def apply(cube: String, state: String, maxWeight: Weight): BlockStats =
    BlockStats(cube, maxWeight, Weight.MinValue, state, 0)

}
