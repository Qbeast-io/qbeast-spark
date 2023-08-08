/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Files represents a physical file to store the index information.
 *
 * @param path the file path
 * @param size the file size in bytes
 * @param modificationTime the last modification timestamp
 */
final case class File(path: String, size: Long, modificationTime: Long) extends Serializable {
  require(path != null)
  require(size >= 0)
}
