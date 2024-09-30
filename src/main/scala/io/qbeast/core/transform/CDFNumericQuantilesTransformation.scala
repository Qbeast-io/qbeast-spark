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
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import io.qbeast.core.model.OrderedDataType
import org.apache.spark.annotation.Experimental

@Experimental
@JsonSerialize(using = classOf[CDFNumericQuantilesTransformationSerializer])
@JsonDeserialize(using = classOf[CDFNumericQuantilesTransformationDeserializer])
case class CDFNumericQuantilesTransformation(
    quantiles: IndexedSeq[Double],
    dataType: OrderedDataType)
    extends CDFQuantilesTransformation {
  require(quantiles.size > 1, "Quantiles size should be greater than 1")

  override def ordering: Ordering[Any] =
    Ordering[Double].asInstanceOf[Ordering[Any]]

  override def mapValue(value: Any): Any = {
    value match {
      case v: Double => v
      case v: Long => v.toDouble
      case v: Int => v.toDouble
      case v: BigDecimal => v.doubleValue()
      case v: Float => v.toDouble
      case v: java.sql.Timestamp => v.getTime.toDouble
      case v: java.sql.Date => v.getTime.toDouble
      case v: java.time.Instant => v.toEpochMilli.toDouble
    }
  }

}

class CDFNumericQuantilesTransformationSerializer
    extends StdSerializer[CDFNumericQuantilesTransformation](
      classOf[CDFNumericQuantilesTransformation]) {
  val jsonFactory = new JsonFactory()

  override def serializeWithType(
      value: CDFNumericQuantilesTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))

    gen.writeFieldName("quantiles")
    gen.writeStartArray()
    value.quantiles.foreach(gen.writeNumber)
    gen.writeEndArray()
    gen.writeObjectField("dataType", value.dataType)
    gen.writeEndObject()
  }

  override def serialize(
      value: CDFNumericQuantilesTransformation,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()

    gen.writeFieldName("quantiles")
    gen.writeStartArray()
    value.quantiles.foreach(gen.writeNumber)
    gen.writeEndArray()

    gen.writeEndObject()
  }

}

class CDFNumericQuantilesTransformationDeserializer
    extends StdDeserializer[CDFNumericQuantilesTransformation](
      classOf[CDFNumericQuantilesTransformation]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): CDFNumericQuantilesTransformation = {
    val tree: TreeNode = p.getCodec.readTree(p)
    // Deserialize the ordered data type
    val odt = tree.get("dataType") match {
      case tn: TextNode => OrderedDataType(tn.asText())
    }
    // Deserialize the quantiles
    val quantilesBuilder = IndexedSeq.newBuilder[Double]
    tree.get("quantiles") match {
      case an: ArrayNode =>
        (0 until an.size()).foreach(i => quantilesBuilder += an.get(i).asDouble())
    }
    CDFNumericQuantilesTransformation(quantilesBuilder.result(), odt)
  }

}
