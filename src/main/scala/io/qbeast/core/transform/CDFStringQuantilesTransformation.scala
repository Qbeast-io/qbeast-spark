package io.qbeast.core.transform

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType

case class CDFStringQuantilesTransformation(quantiles: IndexedSeq[String])
    extends CDFQuantilesTransformation {

  override val dataType: QDataType = StringDataType

  override implicit val ordering: Ordering[Any] =
    implicitly[Ordering[String]].asInstanceOf[Ordering[Any]]

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
