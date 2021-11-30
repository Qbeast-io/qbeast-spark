/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.transform

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.{DoubleNode, IntNode, NumericNode, TextNode}
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import io.qbeast.model.{
  DecimalDataType,
  DoubleDataType,
  IntegerDataType,
  LongDataType,
  OrderedDataType
}
import java.math.BigDecimal

@JsonSerialize(using = classOf[LinearTransformationSerializer])
@JsonDeserialize(using = classOf[LinearTransformationDeserializer])
case class LinearTransformation(minNumber: Any, maxNumber: Any, orderedDataType: OrderedDataType)
    extends Transformation {

  import orderedDataType.ordering._

  private val mn = minNumber.toDouble

  private val scale: Double = {
    val mx = maxNumber.toDouble
    require(mx > mn, "Range cannot be not null, and max must be > min")
    1.0 / (mx - mn)
  }

  override def transform(value: Any): Double = value match {
    case v: Double => (v - mn) * scale
    case v: Long => (v - mn) * scale
    case v: Int => (v - mn) * scale
    case v: BigDecimal => (v.doubleValue() - mn) * scale
    case v: Float => (v - mn) * scale

  }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   * @return a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = {
    other match {
      case LinearTransformation(otherMin, otherMax, otherOrdering)
          if orderedDataType == otherOrdering =>
        LinearTransformation(min(minNumber, otherMin), max(maxNumber, otherMax), orderedDataType)
          .asInstanceOf[Transformation]
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation the new transformation created with statistics over the new data
   * @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case LinearTransformation(newMin, newMax, otherOrdering)
          if orderedDataType == otherOrdering =>
        gt(minNumber, newMin) || lt(maxNumber, newMax)
    }

}

class LinearTransformationSerializer
    extends StdSerializer[LinearTransformation](classOf[LinearTransformation]) {

  private def writeNumb(gen: JsonGenerator, filedName: String, numb: Any): Unit = {
    numb match {
      case v: Double => gen.writeNumberField(filedName, v)
      case v: Long => gen.writeNumberField(filedName, v)
      case v: Int => gen.writeNumberField(filedName, v)
      case v: Float => gen.writeNumberField(filedName, v)
    }
  }

  override def serializeWithType(
      value: LinearTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))

    writeNumb(gen, "minNumber", value.minNumber)
    writeNumb(gen, "maxNumber", value.maxNumber)
    gen.writeObjectField("orderedDataType", value.orderedDataType)
    gen.writeEndObject()
  }

  override def serialize(
      value: LinearTransformation,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    writeNumb(gen, "minNumber", value.minNumber)
    writeNumb(gen, "maxNumber", value.maxNumber)
    gen.writeObjectField("orderedDataType", value.orderedDataType)
    gen.writeEndObject()
  }

}

class LinearTransformationDeserializer
    extends StdDeserializer[LinearTransformation](classOf[LinearTransformation]) {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): LinearTransformation = {
    val json = p.getCodec
    val tree: TreeNode = json
      .readTree(p)

    val odt = tree.get("orderedDataType") match {
      case tn: TextNode => OrderedDataType(tn.asText())
    }
    (odt, tree.get("minNumber"), tree.get("maxNumber")) match {
      case (DoubleDataType, mn: DoubleNode, mx: DoubleNode) =>
        LinearTransformation(mn.asDouble(), mx.asDouble(), odt)
      case (IntegerDataType, mn: IntNode, mx: IntNode) =>
        LinearTransformation(mn.asInt(), mx.asInt(), odt)
      case (LongDataType, mn: NumericNode, mx: NumericNode) =>
        LinearTransformation(mn.asLong(), mx.asLong(), odt)
      case (DecimalDataType, mn: DoubleNode, mx: DoubleNode) =>
        LinearTransformation(mn.doubleValue(), mx.doubleValue(), odt)
      case (a, b, c) =>
        throw new IllegalArgumentException(s"Invalid data type  ($a,$b,$c) ${b.getClass} ")
    }

  }

}
