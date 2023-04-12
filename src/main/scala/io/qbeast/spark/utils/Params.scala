/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import org.apache.spark.sql.functions.col

/**
 * Tag keys for saving qbeast index metadata into the delta commit log
 */
object TagColumns {
  final val cube = col("tags.cube")
  final val minWeight = col("tags.minWeight")
  final val maxWeight = col("tags.maxWeight")
  final val state = col("tags.state")
  final val revision = col("tags.revision")
  final val elementCount = col("tags.elementCount")
}
