/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Index file represents a physical file where blocks of the elements are stored.
 *
 * @param path
 *   the file path
 * @param size
 *   the file size in bytes
 * @param modificationTime
 *   the last modification timestamp
 * @param blocks
 *   the blocks
 */
final class IndexFile private[model] (
    val path: String,
    val size: Long,
    val modificationTime: Long,
    val revisionId: RevisionID,
    val blocks: IISeq[Block])
    extends Serializable {

  /**
   * The number of elements in the file
   *
   * @return
   *   the number of elements
   */
  def elementCount = blocks.map(_.elementCount).sum

  /**
   * Returns whether file contains data from a given cube.
   *
   * @param cubeId
   *   the cube identifier
   * @return
   *   the file contains data of the cube
   */
  def hasCubeData(cubeId: CubeId): Boolean = blocks.exists(_.cubeId == cubeId)

  /**
   * Tries to replicate the blocks that belong to the specified cubes
   *
   * @param the
   *   cube identifiers
   * @return
   *   an instance with corresponding blocks replicated or None if there are no such blocks
   */
  def tryReplicateBlocks(cubeIds: Set[CubeId]): Option[IndexFile] = {
    if (!blocks.exists(block => cubeIds.contains(block.cubeId))) {
      return None
    }
    val newModificationTime = System.currentTimeMillis()
    val newBlocks = blocks.map { block =>
      if (cubeIds.contains(block.cubeId)) block.replicate() else block
    }
    Some(new IndexFile(path, size, newModificationTime, revisionId, newBlocks))
  }

  override def toString(): String =
    s"IndexFile(${path}, ${size}, ${modificationTime}, ${revisionId}, ${blocks})"

}
