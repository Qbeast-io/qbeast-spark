/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.Block
import com.fasterxml.jackson.core.JsonFactory
import java.io.StringWriter
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.JsonParser
import io.qbeast.core.model.File
import io.qbeast.core.model.RowRange
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.Weight
import com.fasterxml.jackson.core.JsonParseException

/**
 * Utility class responsiible for ecoding/decoding sequences of blocks.
 */
private[delta] object BlocksCodec {

  private val jsonFactory = new JsonFactory()

  /**
   * Encodes given blocks into string.
   *
   * @param blocks the blocks to encode
   * @return the encoded blocks
   */
  def encode(blocks: Seq[Block]): String = {
    val writer = new StringWriter()
    val generator = jsonFactory.createGenerator(writer)
    generator.writeStartArray()
    blocks.foreach { block =>
      generator.writeStartObject()
      generator.writeNumberField("from", block.range.from)
      generator.writeNumberField("to", block.range.to)
      generator.writeStringField("cubeId", block.cubeId.string)
      generator.writeStringField("state", block.state)
      generator.writeNumberField("minWeight", block.minWeight.value)
      generator.writeNumberField("max", block.maxWeight.value)
      generator.writeEndObject()
    }
    generator.writeEndArray()
    generator.close()
    writer.close()
    writer.toString()
  }

  /**
   * Decodes the blocks for a given input the file and the number of dimensions.
   *
   * @param file the physical file
   * @param dimensionCount the number of dimensions
   * @param input the input to decode
   * @return the decoded blocks
   */
  def decode(file: File, dimensionCount: Int, input: String): Seq[Block] = {
    val parser = jsonFactory.createParser(input)
    parser.nextToken()
    if (!parser.hasToken(JsonToken.START_ARRAY)) {
      throw new JsonParseException(parser, "Array start is expected")
    }
    parseBlocks(file, dimensionCount, parser)
  }

  private def parseBlocks(file: File, dimensionCount: Int, parser: JsonParser): Seq[Block] = {
    val blocks = Seq.newBuilder[Block]
    parser.nextToken()
    while (parser.hasToken(JsonToken.START_OBJECT)) {
      blocks += parseBlock(file, dimensionCount, parser)
    }
    if (!parser.hasToken(JsonToken.END_ARRAY)) {
      throw new JsonParseException(parser, "Array end is expected")
    }
    parser.nextToken()
    blocks.result()
  }

  private def parseBlock(file: File, dimensionCount: Int, parser: JsonParser): Block = {
    var from: Option[Long] = None
    var to: Option[Long] = None
    var cubeId: Option[CubeId] = None
    var state: Option[String] = None
    var minWeight: Option[Weight] = None
    var maxWeight: Option[Weight] = None
    parser.nextToken()
    while (parser.hasToken(JsonToken.FIELD_NAME)) {
      parser.nextToken()
      if (!parser.hasCurrentToken() || !parser.currentToken().isScalarValue()) {
        throw new JsonParseException(parser, "Field value is expected")
      }
      parser.currentName() match {
        case "from" => from = Some(parser.getValueAsLong())
        case "to" => to = Some(parser.getValueAsLong())
        case "cubeId" => cubeId = Some(CubeId(dimensionCount, parser.getValueAsString()))
        case "state" => state = Some(parser.getValueAsString())
        case "minWeight" => minWeight = Some(Weight(parser.getValueAsInt()))
        case "maxWeight" => maxWeight = Some(Weight(parser.getValueAsInt()))
        case _ => throw new JsonParseException(parser, "Unexpected field")
      }
      parser.nextToken()
    }
    if (!parser.hasToken(JsonToken.END_OBJECT)) {
      throw new JsonParseException(parser, "Object end is expected")
    }
    parser.nextToken()
    Block(file, RowRange(from.get, to.get), cubeId.get, state.get, minWeight.get, maxWeight.get)
  }

}
