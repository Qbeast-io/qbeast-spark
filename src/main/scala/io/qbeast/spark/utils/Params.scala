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
  final val state = col("tags.state")
  final val revision = col("tags.revision")
  final val elementCount = col("tags.elementCount")
}

object TagUtils {
  final val cube = "cube"
  final val minWeight = "minWeight"
  final val maxWeight = "maxWeight"
  final val state = "state"
  final val revision = "revision"
  final val elementCount = "elementCount"
}

object MetadataConfig {
  final val revision = "qbeast.revision"
  final val replicatedSet = "qbeast.replicatedSet"
  final val lastRevisionID = "qbeast.lastRevisionID"
}
