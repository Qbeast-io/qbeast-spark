package io.qbeast.core.model

/**
 * Container class for Qbeast file's metadata
 * @param path
 * @param revision
 * @param cube
 * @param minWeight
 * @param maxWeight
 * @param state
 * @param elementCount
 * @param size
 * @param modificationTime
 */
case class QbeastBlock(
    path: String,
    revision: Long,
    cube: String,
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
    Set("cube", "minWeight", "maxWeight", "state", "revision", "elementCount")

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
      blockMetadata("revision").toLong,
      blockMetadata("cube"),
      Weight(blockMetadata("minWeight").toInt),
      Weight(blockMetadata("maxWeight").toInt),
      blockMetadata("state"),
      blockMetadata("elementCount").toLong,
      size,
      modificationTime)
  }

}
