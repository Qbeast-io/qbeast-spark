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
import org.apache.spark.sql.Column

/**
 * Tag keys for saving qbeast index metadata into the delta commit log
 */
object TagColumns {
  final val revision: Column = col("tags.revision")
}

object TagUtils {
  final val revision = "revision"
  final val blocks = "blocks"
}

object MetadataConfig {
  final val revision = "qbeast.revision"
  final val lastRevisionId = "qbeast.lastRevisionID"
}
