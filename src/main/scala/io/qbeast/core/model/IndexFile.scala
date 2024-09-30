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
case class IndexFile(
    path: String,
    size: Long,
    modificationTime: Long,
    revisionId: RevisionId,
    blocks: IISeq[Block])
    extends Serializable {

  /**
   * The number of elements in the file
   *
   * @return
   *   the number of elements
   */
  def elementCount: Long = blocks.map(_.elementCount).sum

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
   * @param cubeIds
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
    Some(IndexFile(path, size, newModificationTime, revisionId, newBlocks))
  }

  override def toString: String = {
    s"IndexFile($path, $size, $modificationTime, $revisionId, $blocks)"
  }

}
