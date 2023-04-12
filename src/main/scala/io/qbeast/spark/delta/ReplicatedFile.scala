/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.State
import io.qbeast.spark.utils.TagUtils
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
