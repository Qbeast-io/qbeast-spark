package io.qbeast.core.model

/**
 * Container class for Qbeast file's metadata
 *
 * @param path the file path
 * @param cube the cube identifier
 * @param revision the revision identifier
 * @param minWeight the minimum weight of element
 * @param maxWeight the maximum weight of element
 * @param replicated the file is replicated
 * @param elementCount the number of elements
 * @param size the size in bytes
 * @param modificationTime the modification timestamp
 */
case class QbeastBlock(
    path: String,
    cube: String,
    revision: Long,
    minWeight: Weight,
    maxWeight: Weight,
    replicated: Boolean,
    elementCount: Long,
    size: Long,
    modificationTime: Long)

/**
 * Companion object for QbeastBlock
 */
object QbeastBlock {

  private val metadataKeys =
    Set("minWeight", "maxWeight", "replicated", "revision", "elementCount", "cube")

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
      blockMetadata("cube"),
      blockMetadata("revision").toLong,
      Weight(blockMetadata("minWeight").toInt),
      Weight(blockMetadata("maxWeight").toInt),
      blockMetadata("replicated").toBoolean,
      blockMetadata("elementCount").toLong,
      size,
      modificationTime)
  }

}
