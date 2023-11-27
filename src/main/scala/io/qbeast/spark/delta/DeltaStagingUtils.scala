/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.StagingUtils
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, Dataset}

private[spark] trait DeltaStagingUtils extends StagingUtils {
  protected def snapshot: Snapshot

  protected def isInitial: Boolean = snapshot.version == -1

  /**
   * Condition for Staging AddFiles in the form of Spark sql Column
   */
  private val isStagingFile: Column =
    col("tags").isNull.or(col("tags.revision") === lit(stagingID.toString))

  /**
   * Extract current staging files from the snapshot
   * @return
   */
  protected def stagingFiles(): Dataset[AddFile] = {
    snapshot.allFiles.where(isStagingFile)
  }

}
