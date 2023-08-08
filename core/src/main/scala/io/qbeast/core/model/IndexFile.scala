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
final case class IndexFile(file: File, revisionId: Long, blocks: Seq[Block]) {
  require(file != null)
  require(revisionId >= 0)
  require(blocks.nonEmpty)
}
