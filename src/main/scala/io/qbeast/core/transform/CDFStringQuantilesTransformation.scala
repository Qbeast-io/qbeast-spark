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
package io.qbeast.core.transform

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType
import org.apache.spark.annotation.Experimental

@Experimental
@JsonSerialize(using = classOf[CDFStringQuantilesTransformationSerializer])
@JsonDeserialize(using = classOf[CDFStringQuantilesTransformationDeserializer])
case class CDFStringQuantilesTransformation(quantiles: IndexedSeq[String])
    extends CDFQuantilesTransformation {

  require(quantiles.size > 1, "Quantiles size should be greater than 1")

  override val dataType: QDataType = StringDataType

  override def ordering: Ordering[Any] =
    Ordering[String].asInstanceOf[Ordering[Any]]

  override def mapValue(value: Any): Any = {
    value match {
      case v: String => v
      case _ => value.toString
    }
  }

}

class CDFStringQuantilesTransformationSerializer
    extends StdSerializer[CDFStringQuantilesTransformation](
      classOf[CDFStringQuantilesTransformation]) {
  val jsonFactory = new JsonFactory()

  override def serializeWithType(
      value: CDFStringQuantilesTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))

    gen.writeFieldName("quantiles")
    gen.writeStartArray()
    value.quantiles.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
  }

  override def serialize(
      value: CDFStringQuantilesTransformation,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()

    gen.writeFieldName("quantiles")
    gen.writeStartArray()
    value.quantiles.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
  }

}

class CDFStringQuantilesTransformationDeserializer
    extends StdDeserializer[CDFStringQuantilesTransformation](
      classOf[CDFStringQuantilesTransformation]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): CDFStringQuantilesTransformation = {
    val histogramBuilder = IndexedSeq.newBuilder[String]

    val tree: TreeNode = p.getCodec.readTree(p)
    tree.get("quantiles") match {
      case an: ArrayNode =>
        (0 until an.size()).foreach(i => histogramBuilder += an.get(i).asText())
    }

    CDFStringQuantilesTransformation(histogramBuilder.result())
  }

}
