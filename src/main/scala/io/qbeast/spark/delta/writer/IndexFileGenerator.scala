/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.Block
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.File
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.RevisionID
import io.qbeast.core.model.RowRange
import io.qbeast.core.model.Weight
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter

import scala.collection.mutable.Buffer

/**
 * Index file generator writes a single index file in terms of its structure,
 * i.e. blocks, rows etc. After writing when closes it returns the written index
 * file.
 *
 * A typical usage looks like the following:
 * {{{
 * val generator = new IndexFileGenerator(writer, revisionId, config)
 *
 * generator.beginBlock(cubeId1, "FLOODED")
 * generator.writeRow(row1)
 * generator.writeRow(row2)
 * generator.writeRow(row3)
 * generator.endBlock(minWeight1, maxWeight1)
 *
 * generator.beginBlock(cubeId2, "FLOODED")
 * generator.writeRow(row4)
 * generator.writeRow(row5)
 * generator.writeRow(row6)
 * generator.endBlock(minWeight2, maxWeight2)
 *
 * generator.beginBlock(cubeId3, "FLOODED")
 * generator.writeRow(row7)
 * generator.writeRow(row8)
 * generator.writeRow(row9)
 * generator.endBlock(minWeight3, maxWeight3)
 *
 * val file = generator.close()
 * }}}
 *
 * @param writer the output writer
 */
private[writer] class IndexFileGenerator(
    writer: OutputWriter,
    revisionId: RevisionID,
    config: Configuration) {

  private val endedBlocks = Buffer.empty[BlockBuilder]
  private var currentBlock: Option[BlockBuilder] = None
  private var position = 0L

  /**
   * Returns the path of the index file being generated.
   *
   * @return the path of the index file being generated
   */
  def path: String = writer.path()

  /**
   * Begins a new block with given cube identifier and state. It is a
   * programming error if the previous block has not been ended.
   *
   * @param cubeid the cube identifier
   * @param state the cube state
   */
  def beginBlock(cubeId: CubeId, state: String): Unit = {
    if (currentBlock.isDefined) {
      throw new IllegalStateException("The current block must end first")
    }
    currentBlock = Some(new BlockBuilder(position, cubeId, state))
  }

  /**
   * Writes a given row. It is a programming error to write a row out of the
   * block scope.
   *
   * @param row the row to write
   */
  def writeRow(row: InternalRow): Unit = {
    if (currentBlock.isEmpty) {
      throw new IllegalStateException("A block must begin first")
    }
    writer.write(row)
    position += 1
  }

  /**
   * Ends the previous block. It is a programming error to end a non-existing
   * block, i.e. which has not begun before.
   *
   * @param minWeight the minimum weight of row in the ending block
   * @param maxWeight the maximum weight of row in the ending block
   */
  def endBlock(minWeight: Weight, maxWeight: Weight): Unit = {
    if (currentBlock.isEmpty) {
      throw new IllegalStateException("A block must begin first")
    }
    endedBlocks.append(
      currentBlock.get.setTo(position).setMinWeight(minWeight).setMaxWeight(maxWeight))
    currentBlock = None
  }

  /**
   * Closes the generator and returns the written file. It is programming error
   * to close the generator before the latest block ends.
   *
   * @return the written index file
   */
  def close(): IndexFile = {
    if (currentBlock.isDefined) {
      throw new IllegalStateException("The current block must end first")
    }
    writer.close()
    val hadoopPath = new Path(path)
    val status = hadoopPath.getFileSystem(config).getFileStatus(hadoopPath)
    val file = File(hadoopPath.getName(), status.getLen(), status.getModificationTime())
    val blocks = endedBlocks.iterator.map(_.setFile(file).result()).toArray
    IndexFile(file, revisionId, blocks)
  }

  private class BlockBuilder(from: Long, cubeId: CubeId, state: String) {

    private var file: Option[File] = None
    private var to: Option[Long] = None
    private var minWeight: Option[Weight] = None
    private var maxWeight: Option[Weight] = None

    def setFile(file: File): BlockBuilder = {
      this.file = Some(file)
      this
    }

    def setTo(to: Long): BlockBuilder = {
      this.to = Some(to)
      this
    }

    def setMinWeight(minWeight: Weight): BlockBuilder = {
      this.minWeight = Some(minWeight)
      this
    }

    def setMaxWeight(maxWeight: Weight): BlockBuilder = {
      this.maxWeight = Some(maxWeight)
      this
    }

    def result(): Block =
      Block(file.get, RowRange(from, to.get), cubeId, state, minWeight.get, maxWeight.get)

  }

}
