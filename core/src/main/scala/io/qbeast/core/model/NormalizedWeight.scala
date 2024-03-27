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
 * Normalized weight companion object.
 */
object NormalizedWeight {

  /**
   * Creates a normalized weight from a given simple weight.
   *
   * @param weight
   *   the simple weight
   * @return
   *   a normalized weight
   */
  def apply(weight: Weight): NormalizedWeight = weight.fraction

  /**
   * Creates a normalized weight from given desired and actual cube sizes.
   *
   * @param desiredCubeSize
   *   the desired cube size
   * @param cubeSize
   *   the actual cube size, must not be zero
   * @return
   *   a normalized weight
   */
  def apply(desiredCubeSize: Int, cubeSize: Long): NormalizedWeight = {
    if (cubeSize == 0) {
      throw new IllegalArgumentException("Cube size is zero.")
    }
    desiredCubeSize.toDouble / cubeSize
  }

  /**
   * Merges two given weights.
   *
   * @param weight
   *   the weight
   * @param otherWeight
   *   the other weight
   * @return
   *   the merged weight
   */
  def merge(weight: NormalizedWeight, otherWeight: NormalizedWeight): NormalizedWeight =
    if (weight != 0.0 && otherWeight != 0.0) {
      weight * otherWeight / (weight + otherWeight)
    } else {
      math.max(weight, otherWeight)
    }

  /**
   * Converts a given normalized weight to a simple weight.
   *
   * @param weight
   *   the weight to convert
   * @return
   *   the simple weight
   */
  def toWeight(weight: NormalizedWeight): Weight = {
    if (weight < 1.0) Weight(weight) else Weight.MaxValue
  }

}
