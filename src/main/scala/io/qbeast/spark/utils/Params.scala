/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import org.apache.spark.sql.functions.col

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
object TagColumns {
  final val cube = col("tags.cube")
  final val minWeight = col("tags.minWeight")
  final val maxWeight = col("tags.maxWeight")
  final val replicated = col("tags.replicated")
  final val revision = col("tags.revision")
  final val elementCount = col("tags.elementCount")
}

object TagUtils {
  final val cube = "cube"
  final val minWeight = "minWeight"
  final val maxWeight = "maxWeight"
  final val replicated = "replicated"
  final val revision = "revision"
  final val elementCount = "elementCount"
}

object MetadataConfig {
  final val revision = "qbeast.revision"
  final val lastRevisionID = "qbeast.lastRevisionID"
}
