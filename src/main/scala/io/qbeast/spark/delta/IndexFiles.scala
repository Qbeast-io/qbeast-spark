/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import org.apache.spark.sql.delta.actions.AddFile
import io.qbeast.core.model.IndexFile
import io.qbeast.spark.utils.TagUtils
import io.qbeast.IISeq
import io.qbeast.core.model.Block
import io.qbeast.core.model.IndexFileBuilder
import io.qbeast.core.model.IndexFileBuilder.BlockBuilder
import io.qbeast.core.model.CubeId
import java.io.StringWriter
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonParseException
import io.qbeast.core.model.Weight
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.spark.sql.delta.actions.RemoveFile

/**
 * Utility object for working with index files.
 */
object IndexFiles {

  private val jsonFactory = new JsonFactory()

  /**
   * Creates an IndexFile instance from a given AddFile instance.
   *
   * @param dimensionCount
   *   the number of the index dimensions
   * @param addFile
   *   the AddFile instance
   * @return
   *   an IndexFile instance
   */
  def fromAddFile(dimensionCount: Int)(addFile: AddFile): IndexFile = {
    val builder = new IndexFileBuilder()
      .setPath(addFile.path)
      .setSize(addFile.size)
      .setModificationTime(addFile.modificationTime)
    addFile.getTag(TagUtils.revision) match {
      case Some(value) => builder.setRevisionId(value.toLong)
      case None =>
    }
    addFile.getTag(TagUtils.blocks) match {
      case Some(value) => decodeBlocks(value, dimensionCount, builder)
      case None => builder.beginBlock().setCubeId(CubeId.root(dimensionCount)).endBlock()
    }
    builder.result()
  }

  /**
   * Converts a given IndexFile instance to an AddFile instance.
   *
   * @param dataChange
   *   the index file represents a data change
   * @param indexFile
   *   the IndexFile instance
   * @return
   *   an AddFile instance
   */
  def toAddFile(dataChange: Boolean = true)(indexFile: IndexFile): AddFile = {
    val tags = Map(
      TagUtils.revision -> indexFile.revisionId.toString,
      TagUtils.blocks -> encodeBlocks(indexFile.blocks))
    AddFile(
      path = indexFile.path,
      partitionValues = Map.empty[String, String],
      size = indexFile.size,
      modificationTime = indexFile.modificationTime,
      dataChange = dataChange,
      tags = tags)
  }

  /**
   * Converts a given IndexFile instance to a RemoveFile instance.
   *
   * @param dataChange
   *   file removal implies data change
   * @param indexFile
   *   the IndexFile instance
   * @param a
   *   RemoveFile instance
   */
  def toRemoveFile(dataChange: Boolean = false)(indexFile: IndexFile): RemoveFile =
    RemoveFile(
      path = indexFile.path,
      deletionTimestamp = Some(System.currentTimeMillis()),
      dataChange = dataChange,
      partitionValues = Map.empty[String, String],
      size = Some(indexFile.size))

  /**
   * Converts IndexFile instance to FileStatus instance.
   *
   * @param indexPath
   *   the path to the root of the index
   * @param indexFile
   *   the IndexFile instance
   * @return
   *   a FileStatus instance
   */
  def toFileStatus(indexPath: Path)(indexFile: IndexFile): FileStatus = {
    var path = new Path(new URI(indexFile.path))
    if (!path.isAbsolute()) {
      path = new Path(indexPath, path)
    }
    new FileStatus(indexFile.size, false, 0, 0, indexFile.modificationTime, path)
  }

  private def encodeBlocks(blocks: IISeq[Block]): String = {
    val writer = new StringWriter()
    val generator = jsonFactory.createGenerator(writer)
    generator.writeStartArray()
    blocks.foreach { block =>
      generator.writeStartObject()
      generator.writeStringField("cubeId", block.cubeId.string)
      generator.writeNumberField("minWeight", block.minWeight.value)
      generator.writeNumberField("maxWeight", block.maxWeight.value)
      generator.writeNumberField("elementCount", block.elementCount)
      generator.writeBooleanField("replicated", block.replicated)
      generator.writeEndObject()
    }
    generator.writeEndArray()
    generator.close()
    writer.close()
    writer.toString()
  }

  private def decodeBlocks(json: String, dimensionCount: Int, builder: IndexFileBuilder): Unit = {
    val parser = jsonFactory.createParser(json)
    parser.nextToken()
    if (!parser.hasToken(JsonToken.START_ARRAY)) {
      throw new JsonParseException(parser, "Array start is expected")
    }
    parseBlocks(parser, dimensionCount, builder)
    parser.close()
  }

  private def parseBlocks(
      parser: JsonParser,
      dimensionCount: Int,
      builder: IndexFileBuilder): Unit = {
    parser.nextToken()
    while (parser.hasToken(JsonToken.START_OBJECT)) {
      parseBlock(parser, dimensionCount, builder.beginBlock())
      parser.nextToken()
    }
    if (!parser.hasToken(JsonToken.END_ARRAY)) {
      throw new JsonParseException(parser, "Array end is expected")
    }
  }

  private def parseBlock(parser: JsonParser, dimensionCount: Int, builder: BlockBuilder): Unit = {
    parser.nextToken()
    while (parser.hasToken(JsonToken.FIELD_NAME)) {
      parser.nextToken()
      parser.currentName() match {
        case "cubeId" => builder.setCubeId(CubeId(dimensionCount, parser.getValueAsString()))
        case "minWeight" => builder.setMinWeight(Weight(parser.getValueAsInt()))
        case "maxWeight" => builder.setMaxWeight(Weight(parser.getValueAsInt()))
        case "elementCount" => builder.setElementCount(parser.getValueAsLong())
        case "replicated" => builder.setReplicated(parser.getValueAsBoolean())
        case name => throw new JsonParseException(parser, s"Unexpected field '${name}'")
      }
      parser.nextToken()
    }
    if (!parser.hasToken(JsonToken.END_OBJECT)) {
      throw new JsonParseException(parser, "Object end is expected")
    }
    builder.endBlock()
  }

}
