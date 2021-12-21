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
 */
case class QbeastFile(
    path: String,
    revision: Long,
    cube: String,
    minWeight: Weight,
    maxWeight: Weight,
    state: String,
    elementCount: Long)

/**
 * Companion object for QbeastFile
 */
object QbeastFile {

  private val metadataKeys =
    Set("cube", "minWeight", "maxWeight", "state", "revision", "elementCount")

  private def checkFileMetadata(fileMetadata: Map[String, String]): Unit = {
    metadataKeys.foreach(key =>
      if (!fileMetadata.contains(key)) {
        throw new IllegalArgumentException(s"Missing metadata key $key")
      })
  }

  /**
   * Creates a QbeastFile from a file path and metadata map
   * @param path
   * @param fileMetadata
   * @return
   */
  def apply(path: String, fileMetadata: Map[String, String]): QbeastFile = {
    checkFileMetadata(fileMetadata)

    QbeastFile(
      path,
      fileMetadata("revision").toLong,
      fileMetadata("cube"),
      Weight(fileMetadata("minWeight").toInt),
      Weight(fileMetadata("maxWeight").toInt),
      fileMetadata("state"),
      fileMetadata("elementCount").toLong)
  }

}
