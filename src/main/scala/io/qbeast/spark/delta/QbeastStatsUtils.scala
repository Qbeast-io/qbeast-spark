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
package io.qbeast.spark.delta

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import io.qbeast.core.model.mapper
import io.qbeast.core.model.QbeastStats

object QbeastStatsUtils {
  private val module = new SimpleModule()
  module.addSerializer(classOf[String], new ValueSerializer)
  module.addDeserializer(classOf[String], new ValueDeserializer)
  mapper.registerModule(module)

  def fromString(jsonString: String): Option[QbeastStats] = {
    try {
      Some(mapper.asInstanceOf[ClassTagExtensions].readValue[QbeastStats](jsonString))
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

  def toString(qbeastStats: QbeastStats): String = mapper.writeValueAsString(qbeastStats)

}

class ValueSerializer extends JsonSerializer[String] {

  override def serialize(
      value: String,
      gen: JsonGenerator,
      serializers: SerializerProvider): Unit = {
    try {
      val intValue = value.toInt
      gen.writeNumber(intValue)
    } catch {
      case _: NumberFormatException =>
        try {
          val doubleValue = value.toDouble
          gen.writeNumber(doubleValue)
        } catch {
          case _: NumberFormatException =>
            gen.writeString(value)
        }
    }
  }

}

class ValueDeserializer extends JsonDeserializer[String] {

  override def deserialize(p: JsonParser, ct: DeserializationContext): String = {
    val node = p.getCodec.readTree[JsonNode](p)
    if (node.isNumber) {
      node.asText()
    } else if (node.isTextual) {
      node.asText()
    } else {
      throw new IllegalArgumentException("Unsupported JSON type for value")
    }
  }

}
