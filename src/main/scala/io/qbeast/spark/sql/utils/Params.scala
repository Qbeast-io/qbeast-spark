/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.utils

/**
 * Names of possible states of the cube
 */
object State {
  final val FLOODED = "FLOODED"
  final val REPLICATED = "REPLICATED"
  final val ANNOUNCED = "ANNOUNCED"
}

/**
 * Tag keys for saving qbeast index metadata into the delta commit log
 */
object TagUtils {
  final val cube = "cube"
  final val minWeight = "minWeight"
  final val maxWeight = "maxWeight"
  final val state = "state"
  final val space = "spaceRevision"
  final val elementCount = "rowCount"
}

object MetadataConfig {

  final val indexedColumns = "qbeast.indexedColumns"
  final val desiredCubeSize = "qbeast.desiredCubeSize"
  final val revision = "qbeast.revision"
  final val replicatedSet = "qbeast.replicatedSet"
  final val lastRevisionTimestamp = "qbeast.lastRevisionTimestamp"
}
