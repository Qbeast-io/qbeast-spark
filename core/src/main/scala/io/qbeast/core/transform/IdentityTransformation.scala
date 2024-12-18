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

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.NullNode
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import io.qbeast.core.model._

@JsonSerialize(using = classOf[IdentityTransformationSerializer])
@JsonDeserialize(using = classOf[IdentityTransformationDeserializer])
case class IdentityTransformation(identityValue: Any, orderedDataType: OrderedDataType)
    extends Transformation {

  import orderedDataType.ordering._

  override def transform(value: Any): Double = 0.0

  override def isSupersededBy(other: Transformation): Boolean = other match {
    case IdentityTransformation(newIdValue, newType) if newType == orderedDataType =>
      if (newIdValue == null) false
      else if (identityValue == null) true
      else newIdValue != identityValue
    case _: EmptyTransformation => false
    case _ => true
  }

  override def merge(other: Transformation): Transformation = other match {
    case linear @ LinearTransformation(min, max, _, newType) if newType == orderedDataType =>
      if (identityValue == null || (lteq(min, identityValue) && gteq(max, identityValue))) linear
      else {
        val minValue = min.min(identityValue)
        val maxValue = max.max(identityValue)
        LinearTransformation(minValue, maxValue, orderedDataType)
      }
    case identity @ IdentityTransformation(newIdValue, newType) if newType == orderedDataType =>
      if (newIdValue == null) this
      else if (identityValue == null) identity
      else if (identityValue == newIdValue) this
      else {
        val minValue = identityValue.min(newIdValue)
        val maxValue = identityValue.max(newIdValue)
        LinearTransformation(minValue, maxValue, orderedDataType)
      }
    case _: EmptyTransformation => this
    case _ => other
  }

}

class IdentityTransformationSerializer
    extends StdSerializer[IdentityTransformation](classOf[IdentityTransformation]) {

  override def serializeWithType(
      value: IdentityTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))
    value.identityValue match {
      case v: Double => gen.writeNumberField("identityValue", v)
      case v: Long => gen.writeNumberField("identityValue", v)
      case v: Int => gen.writeNumberField("identityValue", v)
      case v: Float => gen.writeNumberField("identityValue", v)
      case null => gen.writeNullField("identityValue")
    }
    gen.writeObjectField("orderedDataType", value.orderedDataType)
    gen.writeEndObject()
  }

  override def serialize(
      value: IdentityTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider): Unit = {
    gen.writeStartObject()
    value.identityValue match {
      case v: Double => gen.writeNumberField("identityValue", v)
      case v: Long => gen.writeNumberField("identityValue", v)
      case v: Int => gen.writeNumberField("identityValue", v)
      case v: Float => gen.writeNumberField("identityValue", v)
    }
    gen.writeObjectField("orderedDataType", value.orderedDataType)
    gen.writeEndObject()
  }

}

class IdentityTransformationDeserializer
    extends StdDeserializer[IdentityTransformation](classOf[IdentityTransformation]) {

  private def getTypedValue(odt: OrderedDataType, tree: TreeNode): Any = {
    (odt, tree) match {
      case (IntegerDataType, int: IntNode) => int.asInt
      case (DoubleDataType, double: DoubleNode) => double.asDouble
      case (LongDataType, long: NumericNode) => long.asLong
      case (FloatDataType, float: DoubleNode) => float.floatValue
      case (DecimalDataType, decimal: DoubleNode) => decimal.asDouble
      case (TimestampDataType, timestamp: NumericNode) => timestamp.asLong
      case (DateDataType, date: NumericNode) => date.asLong
      case (_, _: NullNode) => null
      case (_, null) => null
      case (a, b) =>
        throw new IllegalArgumentException(s"Invalid data type  ($a,$b) ${b.getClass} ")
    }

  }

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): IdentityTransformation = {
    val tree: TreeNode = p.getCodec.readTree(p)
    val odt = tree.get("orderedDataType") match {
      case tn: TextNode => OrderedDataType(tn.asText())
    }
    val identityValue = getTypedValue(odt, tree.get("identityValue"))
    IdentityTransformation(identityValue, odt)
  }

}
