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
  final val revision = col("tags.revision")
}

object TagUtils {
  final val revision = "revision"
  final val blocks = "blocks"
}

object MetadataConfig {
  final val revision = "qbeast.revision"
  final val lastRevisionID = "qbeast.lastRevisionID"
}
