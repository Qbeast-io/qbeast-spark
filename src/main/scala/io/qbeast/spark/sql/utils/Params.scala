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
  final val cubeTag = "cube"
  final val weightMinTag = "minWeight"
  final val weightMaxTag = "maxWeight"
  final val stateTag = "state"
  final val spaceTag = "spaceRevision"
  final val elementCountTag = "rowCount"
}

object MetadataConfig {

  final val metadataIndexedColumns = "qbeast.indexedColumns"
  final val metadataDesiredCubeSize = "qbeast.desiredCubeSize"
  final val metadataRevision = "qbeast.revision"
  final val metadataReplicatedSet = "qbeast.replicatedSet"
  final val metadataLastRevisionTimestamp = "qbeast.lastRevisionTimestamp"
}
