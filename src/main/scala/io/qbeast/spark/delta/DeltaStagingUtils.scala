/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.StagingUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset

private[spark] trait DeltaStagingUtils extends StagingUtils {
  protected def snapshot: Snapshot

  protected def isInitial: Boolean = snapshot.version == -1

  /**
   * Condition for Staging AddFiles in the form of Spark sql Column
   */
  private val isStagingFileColumn: Column =
    col("tags").isNull.or(col("tags.revision") === lit(stagingID.toString))

  /**
   * Extract current staging files from the snapshot
   * @return
   */
  protected def stagingFiles(): Dataset[AddFile] = {
    snapshot.allFiles.where(isStagingFileColumn)
  }

  /**
   * Wether an AddFile is a staging file or not
   * @param a
   *   the AddFile
   * @return
   *   boolean
   */
  def isStagingFile(a: AddFile): Boolean =
    a.tags == null || a.tags.isEmpty || a.tags
      .getOrElse("revision", "") == stagingID.toString

}
