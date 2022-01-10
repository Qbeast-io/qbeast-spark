/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

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
  final val revision = "revision"
  final val elementCount = "elementCount"
  final val protocolVersion = "protocolVersion"
}

object MetadataConfig {
  final val revision = "qbeast.revision"
  final val replicatedSet = "qbeast.replicatedSet"
  final val lastRevisionID = "qbeast.lastRevisionID"
}

/**
 * Qbeast-spark metadata protocol versioning.
 * This value is used to identify the version of the data format.
 * Read more in https://github.com/Qbeast-io/qbeast-spark/issues/44
 * TODO: Add reference to protocol versioning documentation
 */
object QbeastProtocol {
  final val protocolVersion = "2"
}
