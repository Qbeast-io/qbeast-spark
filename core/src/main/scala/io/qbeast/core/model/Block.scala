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
package io.qbeast.core.model

/**
 * Block of elements stored in the physical index file.
 *
 * @param filePath
 *   the path to the file containing the block
 * @param cubeId
 *   the cube identifier
 * @param minWeight
 *   the minimum element weight
 * @param maxWeight
 *   the maximum element weight
 * @param elementCount
 *   the number of elements in the block
 */
case class Block(
    filePath: String,
    cubeId: CubeId,
    minWeight: Weight,
    maxWeight: Weight,
    elementCount: Long)
    extends Serializable {

  override def toString: String = {
    s"Block($filePath, $cubeId, $minWeight, $maxWeight, $elementCount)"
  }

}
