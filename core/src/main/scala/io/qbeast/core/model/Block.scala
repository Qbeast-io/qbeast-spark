/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Block of elements stored in the physical index file.
 *
 * @constructor creates a new instance for given attributes.
 * @param owner the physical file the block belongs to
 * @param revisionId the revision identifier
 * @param cubeId the cube identifier
 * @param minWeight the minimum element weight
 * @param maxWeight the maximum element weight
 * @param replicated the block is replicated
 */
final class Block private[model] (
    private[model] var owner: Option[IndexFile],
    val cubeId: CubeId,
    val minWeight: Weight,
    val maxWeight: Weight,
    val elementCount: Long,
    val replicated: Boolean)
    extends Serializable {

  /**
   * Returns the file.
   *
   * @return the file
   */
  def file: IndexFile = owner.get

  /**
   * Replicates the block.
   *
   * @return the copy of the block with the replicated attribute set to true
   */
  def replicate(): Block =
    if (replicated) this else new Block(owner, cubeId, minWeight, maxWeight, elementCount, true)

  override def equals(obj: Any): Boolean = obj match {
    case other: Block => (
      owner.map(_.path) == other.owner.map(_.path)
        && cubeId == other.cubeId
        && minWeight == other.minWeight
        && maxWeight == other.maxWeight
        && elementCount == other.elementCount
        && replicated == other.replicated
    )
    case _ => false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + owner.map(_.path).hashCode()
    result = prime * result + cubeId.hashCode()
    result = prime * result + minWeight.hashCode()
    result = prime * result + maxWeight.hashCode()
    result = prime * result + elementCount.hashCode()
    result = prime * result + replicated.hashCode()
    result
  }

  override def toString(): String =
    s"Block(${owner.map(_.path)}, ${cubeId}, ${minWeight}, ${maxWeight}, ${elementCount}, ${replicated})"

}
