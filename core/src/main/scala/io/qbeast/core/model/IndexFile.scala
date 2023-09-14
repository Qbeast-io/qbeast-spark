/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Index file represents a collection of blocks which are stored continously in
 * the same physical file.
 *
 * @param file the physical file that stores the index blocks
 * @param revisionId the revision identifier
 * @param blocks the index blocks
 */
final case class IndexFile(file: File, revisionId: RevisionID, blocks: Array[Block])
    extends Serializable {
  require(file != null)
  require(revisionId >= 0)
  require(blocks.nonEmpty)

  /**
   * Returns whether the index file belongs to the specified revision.
   *
   * @param revisionId the revision identifier
   * @return the file belongs to the revision
   */
  def belongsToRevision(revisionId: RevisionID): Boolean = this.revisionId == revisionId

  /**
   * Returns whether the file contains blocks which belong to the specified
   * cubes.
   *
   * @param cubeIds the cube identifiers
   * @param the file contains blocks from the specified cubes
   * @return the file contains blocks of the specified cubes
   */
  def hasBlocksFromCubes(cubeIds: Set[CubeId]): Boolean =
    blocks.exists(block => cubeIds.contains(block.cubeId))

  /**
   * Sets the state to replicated for the blocks, which belong to the specified
   * cubes.
   *
   * @param cubeIds the cube identifiers
   * @return a new instance with the requested blocks set to replicated
   */
  def setBlocksReplicated(cubeIds: Set[CubeId]): IndexFile = {
    val blocks = this.blocks.map { block =>
      if (cubeIds.contains(block.cubeId)) {
        block.copy(state = "REPLICATED")
      } else {
        block
      }
    }
    copy(blocks = blocks)
  }

  override def equals(other: Any): Boolean = other match {
    case IndexFile(file, revisionId, blocks) =>
      this.file == file && revisionId == revisionId && this.blocks.length == blocks.length && (0 until this.blocks.length)
        .forall(i => this.blocks(i) == blocks(i))
    case _ => false
  }

  override def hashCode(): Int = {
    val hash = 31 * file.hashCode() + revisionId.hashCode()
    blocks.foldLeft(hash)((hash, block) => 31 * hash + block.hashCode())
  }

}
