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
package io.qbeast.spark.delta

import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.spark.sql.delta.actions.AddFile

/**
 * AddFile with Replicate information
 */
object ReplicatedFile {

  def apply(addFile: AddFile): AddFile = {
    val newTags = addFile.tags.updated(TagUtils.state, State.REPLICATED)
    addFile.copy(tags = newTags, modificationTime = System.currentTimeMillis())
  }

}
