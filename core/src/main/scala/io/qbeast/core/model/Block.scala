/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Block represents the index data stored as a continous media in a index file.
 * It also provides the necessary metadada to maintain the index structure.
 *
 * @param file the index file
 * @param range the range of the index file that stores the data
 * @param cubeId the cube identifier
 * @param state the block state
 * @param minWeight the minimum weight of the block element
 * @param maxWeight the maximum weight of the block element
 */
final case class Block(
    file: File,
    range: RowRange,
    cubeId: CubeId,
    state: String,
    minWeight: Weight,
    maxWeight: Weight)
    extends Serializable {
  require(file != null)
  require(range != null)
  require(cubeId != null)
  require(state != null)
  require(minWeight <= maxWeight)

  /**
   * Returns the number of the stored elements.
   */
  def elementCount: Long = range.length
}
