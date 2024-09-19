package io.qbeast.core.transform

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import io.qbeast.core.model.OrderedDataType

case class CDFNumericQuantilesTransformation(
    quantiles: IndexedSeq[Double],
    dataType: OrderedDataType)
    extends CDFQuantilesTransformation {

  override implicit val ordering: Ordering[Any] =
    implicitly[Ordering[Double]].asInstanceOf[Ordering[Any]]

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
    val odt = tree.get("orderedDataType") match {
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
