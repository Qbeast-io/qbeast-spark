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
final case class IndexFile(file: File, revisionId: Long, blocks: Array[Block])
    extends Serializable {
  require(file != null)
  require(revisionId >= 0)
  require(blocks.nonEmpty)

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
