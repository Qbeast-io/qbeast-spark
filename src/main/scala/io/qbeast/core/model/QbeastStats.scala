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
package io.qbeast.core.model

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class QbeastStats(
    numRecords: Long,
    minValues: Map[String, Either[Int, String]],
    maxValues: Map[String, Either[Int, String]],
    nullCount: Map[String, Int]) {

  def toJson: String = QbeastStats.mapper.writeValueAsString(this)

  override def toString: String = toJson
}

object QbeastStats {

  private val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  private val module = new SimpleModule()
  module.addSerializer(classOf[Either[Int, String]], new EitherSerializer)
  module.addDeserializer(classOf[Either[Int, String]], new EitherDeserializer)
  mapper.registerModule(module)

  def fromString(jsonString: String): Option[QbeastStats] = {
    try {
      Some(mapper.readValue[QbeastStats](jsonString))
    } catch {
      case e: JsonParseException =>
        println(s"Failed to parse JSON: ${e.getMessage}")
        None
      case e: JsonMappingException =>
        println(s"Error mapping JSON: ${e.getMessage}")
        None
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        None
    }
  }

}

class EitherSerializer extends JsonSerializer[Either[Int, String]] {

  override def serialize(
      value: Either[Int, String],
      gen: JsonGenerator,
      serializers: SerializerProvider): Unit = {
    value match {
      case Left(intValue) => gen.writeNumber(intValue)
      case Right(strValue) => gen.writeString(strValue)
    }
  }

}

class EitherDeserializer extends JsonDeserializer[Either[Int, String]] {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Either[Int, String] = {
    val node = p.getCodec.readTree[JsonNode](p)
    if (node.isNumber) {
      Left(node.asInt())
    } else if (node.isTextual) {
      Right(node.asText())
    } else {
      throw new IllegalArgumentException("Unsupported JSON type for Either")
    }
  }

}
