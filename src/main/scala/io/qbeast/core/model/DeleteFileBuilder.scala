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

/**
 * Builder for creating IndexFile instances.
 */
final class DeleteFileBuilder {
  private var path: Option[String] = None
  private var size: Long = 0L
  private var deletionTime: Long = 0L

  /**
   * Sets the path.
   *
   * @param path
   *   the path to set
   * @return
   *   this instance
   */
  def setPath(path: String): DeleteFileBuilder = {
    this.path = Some(path)
    this
  }

  /**
   * Sets the size in bytes.
   *
   * @param size
   *   the size in bytes
   * @return
   *   this instance
   */
  def setSize(size: Long): DeleteFileBuilder = {
    this.size = size
    this
  }

  /**
   * Sets the deletion time
   *
   * @param deletionTime
   *   the deletion time to set
   * @return
   *   this instance
   */
  def setDeletionTime(deletionTime: Long): DeleteFileBuilder = {
    this.deletionTime = deletionTime
    this
  }

  /**
   * Builds th result.
   *
   * @return
   *   the index file
   */
  def result(): DeleteFile = {
    val filePath = path.get
    DeleteFile(filePath, size, deletionTime)
  }

}
