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

import scala.collection.immutable

/**
 * Builder for creating IndexFile instances.
 */
final class IndexFileBuilder {
  private var path: Option[String] = None
  private var size: Long = 0L
  private var dataChange: Boolean = true
  private var modificationTime: Long = 0L
  private var revisionId: RevisionID = 0L
  private val blocks = immutable.Seq.newBuilder[VolatileBlock]
  private var stats: Option[QbeastStats] = None

  /**
   * Sets the path.
   *
   * @param path
   *   the path to set
   * @return
   *   this instance
   */
  def setPath(path: String): IndexFileBuilder = {
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
  def setSize(size: Long): IndexFileBuilder = {
    this.size = size
    this
  }

  /**
   * Sets the stats.
   *
   * @param stats
   *   the stats
   * @return
   *   this instance
   */
  def setStats(stats: Option[QbeastStats]): IndexFileBuilder = {
    this.stats = stats
    this
  }

  /**
   * Sets the data change.
   *
   * @param dataChange
   *   whether this index file represents a data change
   * @return
   *   this instance
   */
  def setDataChange(dataChange: Boolean): IndexFileBuilder = {
    this.dataChange = dataChange
    this
  }

  /**
   * Sets the modification time
   *
   * @param modificationTime
   *   the modification time to set
   * @return
   *   this instance
   */
  def setModificationTime(modificationTime: Long): IndexFileBuilder = {
    this.modificationTime = modificationTime
    this
  }

  /**
   * Sets the revision identifier.
   *
   * @param revisionId
   *   the revision identifier to set
   * @return
   *   this instance
   */
  def setRevisionId(revisionId: RevisionID): IndexFileBuilder = {
    this.revisionId = revisionId
    this
  }

  /**
   * Begins a new block.
   *
   * @return
   *   a new block builder
   */
  def beginBlock(): IndexFileBuilder.BlockBuilder = new IndexFileBuilder.BlockBuilder(this)

  private def addBlock(block: VolatileBlock): IndexFileBuilder = {
    blocks += block
    this
  }

  /**
   * Builds th result.
   *
   * @return
   *   the index file
   */
  def result(): IndexFile = {
    val filePath = path.get
    IndexFile(
      filePath,
      size,
      dataChange,
      modificationTime,
      revisionId,
      blocks.result().map(_.toBlock(filePath)),
      stats)
  }

}

/**
 * IndexFileBuilder companion object.
 */
object IndexFileBuilder {

  /**
   * Builder for creating Block instances.
   */
  final class BlockBuilder private[IndexFileBuilder] (owner: IndexFileBuilder) {
    private var cubeId: Option[CubeId] = None
    private var minWeight: Option[Weight] = None
    private var maxWeight: Option[Weight] = None
    private var elementCount: Long = 0L

    /**
     * Sets the cube identifier.
     *
     * @param cubeId
     *   identifier to set
     * @return
     *   this instance
     */
    def setCubeId(cubeId: CubeId): BlockBuilder = {
      this.cubeId = Some(cubeId)
      this
    }

    /**
     * Sets the minimum weight.
     *
     * @param minWeight
     *   the minimum weight to set
     * @return
     *   this instance
     */
    def setMinWeight(minWeight: Weight): BlockBuilder = {
      this.minWeight = Some(minWeight)
      this
    }

    /**
     * Updates the minimum weight by setting the value to minimum between the previous value and
     * the specified one.
     *
     * @param minWeight
     *   the minimum weight to set
     * @return
     *   this instance
     */
    def updateMinWeight(minWeight: Weight): BlockBuilder = {
      this.minWeight = this.minWeight match {
        case Some(value) => Some(Weight.min(minWeight, value))
        case None => Some(minWeight)
      }
      this
    }

    /**
     * Sets the maximum weight.
     *
     * @param maxWeight
     *   the maximum weight to set
     * @return
     *   this instance
     */
    def setMaxWeight(maxWeight: Weight): BlockBuilder = {
      this.maxWeight = Some(maxWeight)
      this
    }

    /**
     * Sets the element count.
     *
     * @param elementCount
     *   the elementCount to set
     * @return
     *   this instance
     */
    def setElementCount(elementCount: Long): BlockBuilder = {
      this.elementCount = elementCount
      this
    }

    /**
     * Increments the element count.
     *
     * @return
     *   this instance
     */
    def incrementElementCount(): BlockBuilder = setElementCount(elementCount + 1)

    /**
     * Ends the block.
     *
     * @return
     *   the IndexFileBuilder instance
     */
    def endBlock(): IndexFileBuilder = owner.addBlock(
      VolatileBlock(
        cubeId.get,
        minWeight.getOrElse(Weight.MinValue),
        maxWeight.getOrElse(Weight.MaxValue),
        elementCount))

  }

}

case class VolatileBlock(
    cubeId: CubeId,
    minWeight: Weight,
    maxWeight: Weight,
    elementCount: Long) {

  def toBlock(filePath: String): Block = {
    Block(filePath, cubeId, minWeight, maxWeight, elementCount)
  }

}
