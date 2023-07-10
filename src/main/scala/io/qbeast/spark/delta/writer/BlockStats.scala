/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.Weight

/**
 * Stats of a data block.
 *
 * @param cube the cube
 * @param maxWeight the maximum weight
 * @param minWeight the minimum weight
 * @param replicated the cube block is replicated
 * @param elementCount the number of rows
 */
case class BlockStats protected (
    cube: String,
    maxWeight: Weight,
    minWeight: Weight,
    replicated: Boolean,
    elementCount: Long) {

  /**
   * Update the BlockStats by computing minWeight = min(this.minWeight, minWeight).
   * This also updates the elementCount of the BlocksStats
   * @param minWeight the Weight to compare with the current one
   * @return the updated BlockStats object
   */
  def update(minWeight: Weight): BlockStats = {
    val minW = Weight.min(minWeight, this.minWeight)
    this.copy(elementCount = elementCount + 1, minWeight = minW)
  }

  override def toString: String =
    s"BlocksStats($cube,$maxWeight,$minWeight,$replicated,$elementCount)"

}

/**
 * BlockStats companion object.
 */
object BlockStats {

  /**
   * Use this constructor to build the first BlockStat
   * @param cube the block's cubeId
   * @param replicated the block is replicated
   * @param maxWeight the maxWeight of the block
   * @return a new empty instance of BlockStats
   */
  def apply(cube: String, replicated: Boolean, maxWeight: Weight): BlockStats =
    BlockStats(cube, maxWeight, minWeight = Weight.MaxValue, replicated, 0)

}
