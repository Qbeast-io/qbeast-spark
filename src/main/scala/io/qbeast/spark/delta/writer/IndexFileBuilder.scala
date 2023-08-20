/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.File
import io.qbeast.core.model.IndexFile
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.SerializableConfiguration
import io.qbeast.core.model.TableChanges
import scala.collection.mutable.Queue
import io.qbeast.spark.utils.State
import io.qbeast.core.model.Weight
import io.qbeast.core.model.Revision
import io.qbeast.spark.index.QbeastColumns

/**
 * Builder for creating IndexFile instances while writing the indexed data.
 *
 * @path the path of the index file being written
 */
private[writer] class IndexFileBuilder(
    path: Path,
    changes: TableChanges,
    qbeastColumns: QbeastColumns,
    configuration: SerializableConfiguration) {

  private var file: Option[File] = None
  private val blockBuilders = Queue.empty[BlockBuilder]
  private var cubeIdBytes: Option[Array[Byte]] = None
  private var position: Long = 0

  /**
   * Updates the state after a given extended row was written.
   *
   * @param extendedRow the written extended row containing the Qbeast-specific
   * fields
   * @return this instance
   */
  def rowWritten(extendedRow: InternalRow): IndexFileBuilder = {
    val cubeIdBytes = getCubeIdBytes(extendedRow)
    if (this.cubeIdBytes.isEmpty || !this.cubeIdBytes.get.sameElements(cubeIdBytes)) {
      this.cubeIdBytes = Some(cubeIdBytes)
      blockBuilders += newBlockBuilder(cubeIdBytes)
    }
    blockBuilders.last.rowWritten(extendedRow)
    position += 1
    this
  }

  /**
   * Updates the state after the index file was written.
   *
   * @return this instance
   */
  def fileWritten(): IndexFileBuilder = {
    val status = path.getFileSystem(configuration.value).getFileStatus(path)
    val file = File(path.getName(), status.getLen(), status.getModificationTime())
    this.file = Some(file)
    blockBuilders.foreach(_.fileWritten(file))
    this
  }

  /**
   * Builds the index file.
   *
   * @return the index file
   */
  def result(): IndexFile = {
    require(file.isDefined)
    IndexFile(file.get, revision.revisionID, blockBuilders.map(_.result()).toSeq)
  }

  private def getCubeIdBytes(extendedRow: InternalRow): Array[Byte] = {
    extendedRow.getBinary(qbeastColumns.cubeColumnIndex)
  }

  private def newBlockBuilder(cubeIdBytes: Array[Byte]): BlockBuilder = {
    val cubeId = revision.createCubeId(cubeIdBytes)
    val state = changes.cubeState(cubeId).getOrElse(State.FLOODED)
    val maxWeight = changes.cubeWeights(cubeId).getOrElse(Weight.MaxValue)
    new BlockBuilder(cubeId, position, state, maxWeight, qbeastColumns)
  }

  private def revision: Revision = changes.updatedRevision
}
