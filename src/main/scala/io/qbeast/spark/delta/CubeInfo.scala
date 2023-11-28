/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.Weight

/**
 * Cube Information
 *
 * @param cube
 *   Id of the cube
 * @param maxWeight
 *   Maximum maxWeight of the cube
 * @param size
 *   Number of elements of the cube
 */

case class CubeInfo(cube: String, maxWeight: Weight, size: Long)
