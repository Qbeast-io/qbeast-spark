/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.Block
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.File
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.RowRange
import io.qbeast.core.model.Weight
import io.qbeast.spark.utils.State
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.actions.RemoveFile

import scala.util.Success
import scala.util.Try

/**
 * Utility object for working with IndexFile instances. This object provides
 * functions that are Delta-specific and therefore cannot be defined directly
 * in the IndexFile or its companion object because thet are parts of the core.
 */
private[delta] object IndexFiles {

  /**
   * Creates an index file from a given AddFile instance.
   *
   * @param dimensionCount the number of dimensions
   * @param addFile the AddFile instance
   * @return the index file
   */
  def fromAddFile(dimensionCount: Int)(addFile: AddFile): IndexFile = {
    val file = getFile(addFile)
    val revisionId = getRevisionId(addFile)
    val blocks = getBlocks(file, dimensionCount, addFile)
    IndexFile(file, revisionId, blocks)
  }

  private def getFile(addFile: AddFile): File =
    File(addFile.path, addFile.size, addFile.modificationTime)

  private def getRevisionId(addFile: AddFile): Long = addFile
    .getTag(TagUtils.revisionId)
    .map(s => Try(s.toLong))
    .collect { case Success(revisionId) => revisionId }
    .getOrElse(0)

  private def getBlocks(file: File, dimensionCount: Int, addFile: AddFile): Array[Block] = {
    addFile.getTag(TagUtils.blocks) match {
      case Some(tag) => BlocksCodec.decode(file, dimensionCount, tag)
      case None =>
        // a staging area file treated as a single block from the root cube
        val to = addFile.numLogicalRecords.getOrElse(Long.MaxValue)
        Array(
          Block(
            file,
            RowRange(0, to),
            CubeId.root(dimensionCount),
            State.FLOODED,
            Weight.MinValue,
            Weight.MaxValue))
    }
  }

  /**
   * Converts a given index file into a AddFile instance.
   *
   * @param the index file
   * @return the AddFile instance
   */
  def toAddFile(indexFile: IndexFile): AddFile = {
    val file = indexFile.file
    val tags = Map(
      TagUtils.revisionId -> indexFile.revisionId.toString(),
      TagUtils.blocks -> BlocksCodec.encode(indexFile.blocks))
    new AddFile(
      path = file.path,
      partitionValues = Map.empty,
      size = file.size,
      modificationTime = file.modificationTime,
      dataChange = true,
      stats = "",
      tags = tags)
  }

  /**
   * Converts a given index file into a RemoveFile instane.
   *
   * @param the index file
   * @return the RemoveFile instance
   */
  def toRemoveFile(indexFile: IndexFile): RemoveFile = {
    val file = indexFile.file
    new RemoveFile(
      path = file.path,
      deletionTimestamp = Some(System.currentTimeMillis()),
      dataChange = false,
      partitionValues = Map.empty,
      size = Some(file.size))
  }

}
