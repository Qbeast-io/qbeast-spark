/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.hudi

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.JsonNode
import io.qbeast.core.model._
import io.qbeast.core.model.IndexFileBuilder.BlockBuilder
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.TagUtils
import io.qbeast.IISeq
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodieWriteStat

import java.io.StringWriter
import scala.collection.JavaConverters._

/**
 * Utility object for working with Hudi commit metadata to create IndexFile instances.
 */
object HudiQbeastFileUtils {

  private val jsonFactory = new JsonFactory()

  /**
   * Creates a list of IndexFile instances from HoodieCommitMetadata.
   *
   * @param dimensionCount
   *   The number of index dimensions
   * @param commitMetadata
   *   The HoodieCommitMetadata instance
   * @return
   *   A list of IndexFile instances
   */
  def fromCommitFile(dimensionCount: Int)(
      commitMetadata: HoodieCommitMetadata): Seq[IndexFile] = {
    commitMetadata.getPartitionToWriteStats.asScala.flatMap { case (_, writeStats) =>
      writeStats.asScala.map { stat =>
        fromWriteStat(dimensionCount, stat, commitMetadata.getExtraMetadata.asScala.toMap)
      }
    }.toSeq
  }

  /**
   * Creates an IndexFile instance from a HoodieWriteStat.
   *
   * @param dimensionCount
   *   The number of index dimensions
   * @param stat
   *   The HoodieWriteStat instance
   * @param extraMetadata
   *   The extra metadata map containing Qbeast-specific information
   * @return
   *   An IndexFile instance
   */
  private def fromWriteStat(
      dimensionCount: Int,
      stat: HoodieWriteStat,
      extraMetadata: Map[String, String]): IndexFile = {

    val path = stat.getPath
    val size = stat.getFileSizeInBytes
    val modificationTime = FSUtils.getCommitTime(path).toLong

    val builder = new IndexFileBuilder()
      .setPath(path)
      .setSize(size)
      .setModificationTime(modificationTime)

    extraMetadata.get(MetadataConfig.blocks) match {
      case Some(value) =>
        val blocks = mapper.readTree(value).get(path)
        builder.setRevisionId(blocks.get(TagUtils.revision).asLong())
        decodeBlocks(blocks, dimensionCount, builder)
      case None =>
        builder.beginBlock().setCubeId(CubeId.root(dimensionCount)).endBlock()
    }

    builder.result()
  }

  /**
   * Converts a single IndexFile instance to a WriteStatus.
   *
   * @param indexFile
   *   The IndexFile instance
   * @param totalWriteTime
   *   Total time taken for the write operation (for RuntimeStats)
   * @return
   *   A WriteStatus object populated with data from the IndexFile
   */
  def toWriteStat(indexFile: IndexFile, totalWriteTime: Long): WriteStatus = {
    val writeStatus = new WriteStatus()
    writeStatus.setFileId(FSUtils.getFileId(indexFile.path))
    writeStatus.setPartitionPath("")
    writeStatus.setTotalRecords(indexFile.elementCount)

    val stat = new HoodieWriteStat()
    stat.setPartitionPath(writeStatus.getPartitionPath)
    stat.setNumWrites(writeStatus.getTotalRecords)
    stat.setNumDeletes(0)
    stat.setNumInserts(writeStatus.getTotalRecords)
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT)
    stat.setFileId(writeStatus.getFileId)
    stat.setPath(indexFile.path)
    stat.setTotalWriteBytes(indexFile.size)
    stat.setFileSizeInBytes(indexFile.size)
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords)

    val runtimeStats = new HoodieWriteStat.RuntimeStats
    runtimeStats.setTotalCreateTime(totalWriteTime)
    stat.setRuntimeStats(runtimeStats)

    writeStatus.setStat(stat)

    writeStatus
  }

  /**
   * Encodes the list of blocks into a JSON string.
   *
   * @param blocks
   *   The list of blocks to encode
   * @return
   *   JSON string representation of the blocks
   */
  def encodeBlocks(blocks: IISeq[Block]): String = {
    val writer = new StringWriter()
    val generator = jsonFactory.createGenerator(writer)
    generator.writeStartArray()
    blocks.foreach { block =>
      generator.writeStartObject()
      generator.writeStringField("cubeId", block.cubeId.string)
      generator.writeNumberField("minWeight", block.minWeight.value)
      generator.writeNumberField("maxWeight", block.maxWeight.value)
      generator.writeNumberField("elementCount", block.elementCount)
      generator.writeEndObject()
    }
    generator.writeEndArray()
    generator.close()
    writer.close()
    writer.toString
  }

  private def decodeBlocks(
      blocksNode: JsonNode,
      dimensionCount: Int,
      builder: IndexFileBuilder): Unit = {

    val blocksArray = blocksNode.get(TagUtils.blocks)
    if (blocksArray == null || !blocksArray.isArray) {
      throw new IllegalArgumentException("Expected 'blocks' array in JSON node")
    }

    blocksArray.forEach { blockNode =>
      parseBlock(blockNode, dimensionCount, builder.beginBlock())
    }
  }

  private def parseBlock(
      blockNode: JsonNode,
      dimensionCount: Int,
      builder: BlockBuilder): Unit = {
    val fields = blockNode.fields()
    while (fields.hasNext) {
      val entry = fields.next()
      entry.getKey match {
        case "cubeId" => builder.setCubeId(CubeId(dimensionCount, entry.getValue.asText()))
        case "minWeight" => builder.setMinWeight(Weight(entry.getValue.asInt()))
        case "maxWeight" => builder.setMaxWeight(Weight(entry.getValue.asInt()))
        case "elementCount" => builder.setElementCount(entry.getValue.asLong())
        case _ => throw new JsonParseException(null, s"Unexpected field '${entry.getKey}'")
      }
    }
    builder.endBlock()
  }

}
