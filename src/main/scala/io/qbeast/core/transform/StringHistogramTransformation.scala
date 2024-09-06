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
import io.qbeast.core.transform.StringHistogramTransformer.defaultStringHistogram

import scala.collection.Searching._

@JsonSerialize(using = classOf[StringHistogramTransformationSerializer])
@JsonDeserialize(using = classOf[StringHistogramTransformationDeserializer])
case class StringHistogramTransformation(histogram: IndexedSeq[String]) extends Transformation {
  require(histogram.length > 1, s"Histogram length has to be > 1: ${histogram.length}")

  val dataType: QDataType = StringDataType

  def isDefault: Boolean = histogram == defaultStringHistogram

  /**
   * Converts a real number to a normalized value.
   *
   * @param value
   *   a real number to convert
   * @return
   *   a real number between 0 and 1
   */
  override def transform(value: Any): Double = {
    val v: String = value match {
      case s: String => s
      case null => "null"
      case _ => value.toString
    }

    histogram.search(v) match {
      case Found(foundIndex) => foundIndex.toDouble / (histogram.length - 1)
      case InsertionPoint(insertionPoint) =>
        if (insertionPoint == 0) 0d
        else if (insertionPoint == histogram.length + 1) 1d
        else (insertionPoint - 1).toDouble / (histogram.length - 1)
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case nt @ StringHistogramTransformation(hist) =>
        if (isDefault) !nt.isDefault
        else if (nt.isDefault) false
        else !(histogram == hist)
      case _ => false
    }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   *   Transformation
   * @return
   *   a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = other match {
    case _: StringHistogramTransformation => other
    case _ => this
  }

}

class StringHistogramTransformationSerializer
    extends StdSerializer[StringHistogramTransformation](classOf[StringHistogramTransformation]) {
  val jsonFactory = new JsonFactory()

  override def serializeWithType(
      value: StringHistogramTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))

    gen.writeFieldName("histogram")
    gen.writeStartArray()
    value.histogram.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
  }

  override def serialize(
      value: StringHistogramTransformation,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()

    gen.writeFieldName("histogram")
    gen.writeStartArray()
    value.histogram.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
  }

}

class StringHistogramTransformationDeserializer
    extends StdDeserializer[StringHistogramTransformation](
      classOf[StringHistogramTransformation]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): StringHistogramTransformation = {
    val histogramBuilder = IndexedSeq.newBuilder[String]

    val tree: TreeNode = p.getCodec.readTree(p)
    tree.get("histogram") match {
      case an: ArrayNode =>
        (0 until an.size()).foreach(i => histogramBuilder += an.get(i).asText())
    }

    StringHistogramTransformation(histogramBuilder.result())
  }

}
