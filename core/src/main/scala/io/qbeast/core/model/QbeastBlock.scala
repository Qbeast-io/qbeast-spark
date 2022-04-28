package io.qbeast.core.model

/**
 * Container class for Qbeast file's metadata extended version
 *
 * @param path
 * @param revision
 * @param minWeight
 * @param maxWeight
 * @param state
 * @param elementCount
 * @param size
 * @param modificationTime
 * @param blockStats
 * @param blockMetadata
 */

case class QbeastBlockExtended(
    path: String,
    revision: Long,
    minWeight: Weight,
    maxWeight: Weight,
    state: String,
    elementCount: Long,
    size: Long,
    modificationTime: Long,
    blockMetadata: Map[String, String] = Map.empty,
    blockStats: Map[String, String] = Map.empty)

object QbeastBlockExtended {

  private val metadataKeys =
    Set("minWeight", "maxWeight", "state", "revision", "elementCount")

  private def checkBlockMetadata(blockMetadata: Map[String, String]): Unit = {
    metadataKeys.foreach(key =>
      if (!blockMetadata.contains(key)) {
        throw new IllegalArgumentException(s"Missing metadata key $key")
      })
  }

  /**
   * Creates a QbeastBlock from a file path and metadata + stats map
   *
   * @param path
   * @param blockMetadata
   * @param blockStats
   * @param size
   * @param modificationTime
   * @return
   */
  def apply(
      path: String,
      blockMetadata: Map[String, String],
      blockStats: Map[String, String],
      size: Long,
      modificationTime: Long): QbeastBlockExtended = {
    checkBlockMetadata(blockMetadata)
    // checkBlockStats(blockStats)

    QbeastBlockExtended(
      path,
      blockMetadata("revision").toLong,
      Weight(blockMetadata("minWeight").toInt),
      Weight(blockMetadata("maxWeight").toInt),
      blockMetadata("state"),
      blockMetadata("elementCount").toLong,
      size,
      modificationTime,
      blockMetadata.map(identity),
      blockStats.map(identity))
  }

}

case class QbeastBlock(
    path: String,
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
    Set("minWeight", "maxWeight", "state", "revision", "elementCount")

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
      Weight(blockMetadata("minWeight").toInt),
      Weight(blockMetadata("maxWeight").toInt),
      blockMetadata("state"),
      blockMetadata("elementCount").toLong,
      size,
      modificationTime)
  }

}
