/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.writer

import io.qbeast.core.model.{Weight}

/**
 * Stats of a data block.
 *
 * @param cube the cube
 * @param maxWeight the maximum weight
 * @param minWeight the minimum weight
 * @param state the cube state
 * @param elementCount the number of rows
 */
case class BlockStats protected (
    cube: String,
    maxWeight: Weight,
    minWeight: Weight,
    state: String,
    elementCount: Long,
    stats: Map[String, ColumnStatsCollection]) {

  def min(v1: Any, v2: Any): Any = (v1, v2) match {
    case (v1: Int, v2: Int) => (v1.min(v2))
    case (v1: Long, v2: Long) => (v1.min(v2))
    case (v1: Double, v2: Double) => (v1.min(v2))
    case (v1: Float, v2: Float) => (v1.min(v2))
    case (v1: Short, v2: Short) => (v1.min(v2))
    case (v1: Byte, v2: Byte) => (v1.min(v2))
    case (v1: BigDecimal, v2: BigDecimal) => (v1.min(v2))
    case (v1: BigInt, v2: BigInt) => (v1.min(v2))
    case _ => null
  }

  def max(v1: Any, v2: Any): Any = (v1, v2) match {
    case (v1: Int, v2: Int) => v1.max(v2)
    case (v1: Long, v2: Long) => (v1.max(v2))
    case (v1: Double, v2: Double) => (v1.max(v2))
    case (v1: Float, v2: Float) => (v1.max(v2))
    case (v1: Short, v2: Short) => (v1.max(v2))
    case (v1: Byte, v2: Byte) => (v1.max(v2))
    case (v1: BigDecimal, v2: BigDecimal) => (v1.max(v2))
    case (v1: BigInt, v2: BigInt) => (v1.max(v2))
    case _ => null
  }

  /**
   * Update the BlockStats by computing minWeight = min(this.minWeight, minWeight).
   * This also updates the elementCount of the BlocksStats
   * @param values the values to update
   * @param minWeight the Weight to compare with the current one
   * @return the updated BlockStats object
   */
  def update(values: Map[String, Any], minWeight: Weight): BlockStats = {
    val minW = Weight.min(minWeight, this.minWeight)
    val newStats =
      if (stats.isEmpty) {
        values.map(k => (k._1, ColumnStatsCollection(k._2, k._2, if (k._2 == null) 0 else 1)))
      } else {
        stats.map { case (k, v) =>
          val newValue = values(k)
          val newMin = min(v.min, newValue)
          val newMax = max(v.max, newValue)
          val nullCounter = if (newValue == null) v.nullCount + 1 else v.nullCount
          (k, ColumnStatsCollection(newMin, newMax, nullCounter))
        }
      }
    this.copy(elementCount = elementCount + 1, minWeight = minW, stats = newStats)
  }

  override def toString: String =
    s"BlocksStats($cube,$maxWeight,$minWeight,$state,$elementCount)"

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
    BlockStats(cube, maxWeight, minWeight = Weight.MaxValue, state, 0, Map.empty)

}
