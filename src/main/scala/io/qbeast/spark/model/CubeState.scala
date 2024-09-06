package io.qbeast.spark.model

/**
 * Names of possible states of the cube
 */

object CubeState {
  type CubeStateValue = String
  final val FLOODED: CubeStateValue = "FLOODED"
  final val ANNOUNCED: CubeStateValue = "ANNOUNCED"}
