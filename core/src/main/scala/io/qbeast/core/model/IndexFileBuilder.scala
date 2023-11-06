/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import scala.collection.immutable

/**
 * Builder for creating IndexFile instances.
 */
final class IndexFileBuilder {
  private var path: Option[String] = None
  private var size: Long = 0L
  private var modificationTime: Long = 0L
  private var revisionId: RevisionID = 0L
  private val blocks = immutable.Seq.newBuilder[Block]

  /**
   * Sets the path.
   *
   * @param path the path to set
   * @return this instance
   */
  def setPath(path: String): IndexFileBuilder = {
    this.path = Some(path)
    this
  }

  /**
   * Sets the size in bytes.
   *
   * @param size the size in bytes
   * @return this instance
   */
  def setSize(size: Long): IndexFileBuilder = {
    this.size = size
    this
  }

  /**
   * Sets the modification time
   *
   * @param modificationTime the modification time to set
   * @return this instance
   */
  def setModificationTime(modificationTime: Long): IndexFileBuilder = {
    this.modificationTime = modificationTime
    this
  }

  /**
   * Sets the revision identifier.
   *
   * @param revisionId the revision identifier to set
   * @return this instance
   */
  def setRevisionId(revisionId: RevisionID): IndexFileBuilder = {
    this.revisionId = revisionId
    this
  }

  /**
   * Begins a new block.
   *
   * @return a new block builder
   */
  def beginBlock(): IndexFileBuilder.BlockBuilder = new IndexFileBuilder.BlockBuilder(this)

  private def addBlock(block: Block): IndexFileBuilder = {
    blocks += block
    this
  }

  /**
   * Builds th result.
   *
   * @return the index file
   */
  def result(): IndexFile = {
    val file = new IndexFile(path.get, size, modificationTime, revisionId, blocks.result())
    file.blocks.foreach(_.owner = Some(file))
    file
  }

}

/**
 * IndexFileBuilder companion object.
 */
object IndexFileBuilder {

  /**
   * Builder for creating Blockk instances.
   */
  final class BlockBuilder private[IndexFileBuilder] (owner: IndexFileBuilder) {
    private var cubeId: Option[CubeId] = None
    private var minWeight: Option[Weight] = None
    private var maxWeight: Option[Weight] = None
    private var elementCount: Long = 0L
    private var replicated: Boolean = false

    /**
     * Sets the cube identifier.
     *
     * @param the cube identifier to set
     * @return this instance
     */
    def setCubeId(cubeId: CubeId): BlockBuilder = {
      this.cubeId = Some(cubeId)
      this
    }

    /**
     * Sets the minimum weight.
     *
     * @param minWeight the minimum weight to set
     * @return this instance
     */
    def setMinWeight(minWeight: Weight): BlockBuilder = {
      this.minWeight = Some(minWeight)
      this
    }

    /**
     * Updates the minimum weight by setting the value to minimum between the
     * previous value and the specified one.
     *
     * @param minWeight the minimum weight to set
     * @return this instance
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
     * @param maxWeight the maximum weight to set
     * @return this instance
     */
    def setMaxWeight(maxWeight: Weight): BlockBuilder = {
      this.maxWeight = Some(maxWeight)
      this
    }

    /**
     * Sets the element count.
     *
     * @param elemenCount the elemenCount to set
     * @return this instance
     */
    def setElemenCount(elementCount: Long): BlockBuilder = {
      this.elementCount = elementCount
      this
    }

    /**
     * Increments the element count.
     *
     * @return this instance
     */
    def incrementElemenCount(): BlockBuilder = setElemenCount(elementCount + 1)

    /**
     * Sets replicated flag
     *
     * @param replicated the replicated flag value to set
     */
    def setReplicated(replicated: Boolean): BlockBuilder = {
      this.replicated = replicated
      this
    }

    /**
     * Ends the block.
     *
     * @return the IndexFileBuilder instance
     */
    def endBlock(): IndexFileBuilder = owner.addBlock(
      new Block(
        None,
        cubeId.get,
        minWeight.getOrElse(Weight.MinValue),
        maxWeight.getOrElse(Weight.MaxValue),
        elementCount,
        replicated))

  }

}
