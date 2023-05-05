package io.qbeast.core.model

/**
 * Container class for Qbeast file's metadata
 * @param path
 * @param cube
 * @param revision
 * @param minWeight
 * @param maxWeight
 * @param state
 * @param elementCount
 * @param size
 * @param modificationTime
 */

case class QbeastBlock(
    path: String,
    cubeId: String,
    revision: Long,
    minWeight: Weight,
    maxWeight: Weight,
    state: String,
    elementCount: Long,
    size: Long,
    modificationTime: Long)

/**
 * Companion object for QbeastBlock
 */
object QbeastBlock {

  private val metadataKeys =
    Set("minWeight", "maxWeight", "state", "revision", "elementCount", "cubeId")

  private def checkBlockMetadata(blockMetadata: Map[String, String]): Unit = {
    metadataKeys.foreach(key =>
      if (!blockMetadata.contains(key)) {
        throw new IllegalArgumentException(s"Missing metadata key $key")
      })
  }

  /**
   * Creates a QbeastBlock from a file path and metadata map
   * @param path
   * @param blockMetadata
   * @param size
   * @param modificationTime
   * @return
   */
  def apply(
      path: String,
      blockMetadata: Map[String, String],
      size: Long,
      modificationTime: Long): QbeastBlock = {
    checkBlockMetadata(blockMetadata)

    QbeastBlock(
      path,
      blockMetadata("cubeId"),
      blockMetadata("revision").toLong,
      Weight(blockMetadata("minWeight").toInt),
      Weight(blockMetadata("maxWeight").toInt),
      blockMetadata("state"),
      blockMetadata("elementCount").toLong,
      size,
      modificationTime)
  }

}
