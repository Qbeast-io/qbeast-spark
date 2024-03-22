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
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import io.qbeast.core.model.Weight

/**
 * Stats of a data block.
 *
 * @param cube
 *   the cube
 * @param maxWeight
 *   the maximum weight
 * @param minWeight
 *   the minimum weight
 * @param state
 *   the cube state
 * @param elementCount
 *   the number of rows
 */
case class BlockStats protected (
    cube: CubeId,
    maxWeight: Weight,
    minWeight: Weight,
    state: String,
    elementCount: Long) {

  /**
   * Update the BlockStats by computing minWeight = min(this.minWeight, minWeight). This also
   * updates the elementCount of the BlocksStats
   * @param minWeight
   *   the Weight to compare with the current one
   * @return
   *   the updated BlockStats object
   */
  def update(minWeight: Weight): BlockStats = {
    val minW = Weight.min(minWeight, this.minWeight)
    this.copy(elementCount = elementCount + 1, minWeight = minW)
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
   * @param cube
   *   the block's cubeId
   * @param state
   *   the status of the block
   * @param maxWeight
   *   the maxWeight of the block
   * @return
   *   a new empty instance of BlockStats
   */
  def apply(cube: CubeId, state: String, maxWeight: Weight): BlockStats =
    BlockStats(cube, maxWeight, minWeight = Weight.MaxValue, state, 0)

}
