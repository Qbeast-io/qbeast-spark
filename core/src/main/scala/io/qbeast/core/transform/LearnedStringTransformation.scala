/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}

@JsonSerialize(using = classOf[LearnedStringTransformationSerializer])
@JsonDeserialize(using = classOf[LearnedStringTransformationDeserializer])
case class LearnedStringTransformation(minString: String, maxString: String)
    extends Transformation {
  private val stringCDF: LearnedBoostingCDF = DefaultLearnedCDF()
  private val (mn, mx) = computeNumericBoundaries()
  private val scale: Double = 1.0 / (mx - mn)
  // The used model may not have enough resolution to distinguish mn and mx,
  // in such cases, we map all values to mn.
  private val isIdentityMapping: Boolean = mn == mx

  private def computeNumericBoundaries(): (Double, Double) = {
    val mn: Double = stringCDF.predict(minString, 0f, 1f)
    val mx: Double = stringCDF.predict(maxString, 0f, 1f)
    if (mx < mn) (mx, mn)
    else (mn, mx)
  }

  private def transformInputType(value: Any): String = value match {
    case null => "null"
    case s: String => s
    case a => a.toString
  }

  private def scaleTransformation(value: Double): Double = {
    (value - mn) * scale
  }

  override def transformColumn(column: Seq[Any]): Seq[Double] = {
    if (isIdentityMapping) Seq.fill(column.size)(mn)
    else {
      val strSeq = column.map(transformInputType)
      stringCDF.predict(strSeq, mn, mx).map(scaleTransformation)
    }
  }

  /**
   * Converts a real number to a normalized value.
   *
   * @param value a real number to convert
   * @return a real number between 0 and 1
   */
  override def transform(value: Any): Double = {
    if (isIdentityMapping) mn
    else {
      val v: String = transformInputType(value)
      if (v <= minString) 0.0
      else if (v >= maxString) 1.0
      else {
        val t = stringCDF.predict(v, mn, mx)
        scaleTransformation(t)
      }
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
      case t: LearnedStringTransformation =>
        t.minString < minString || t.maxString > maxString
      case _ => false
    }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other Transformation
   * @return a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = other match {
    case t: LearnedStringTransformation =>
      val minS = if (minString < t.minString) minString else t.minString
      val maxS = if (maxString > t.maxString) maxString else t.maxString
      LearnedStringTransformation(minS, maxS)
    case _: IdentityToZeroTransformation => this.copy(minString = " ")
  }

}

class LearnedStringTransformationSerializer
    extends StdSerializer[LearnedStringTransformation](classOf[LearnedStringTransformation]) {

  override def serializeWithType(
      value: LearnedStringTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))
    gen.writeStringField("minString", value.minString)
    gen.writeStringField("maxString", value.maxString)
    gen.writeEndObject()
  }

  override def serialize(
      value: LearnedStringTransformation,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()
    gen.writeStringField("minString", value.minString)
    gen.writeStringField("maxString", value.maxString)
    gen.writeEndObject()
  }

}

class LearnedStringTransformationDeserializer
    extends StdDeserializer[LearnedStringTransformation](classOf[LearnedStringTransformation]) {

  private def getText(tn: TreeNode): String = tn match {
    case tn: TextNode => tn.asText()
  }

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): LearnedStringTransformation = {
    val tree: TreeNode = p.getCodec.readTree(p)
    val minString = getText(tree.get("minString"))
    val maxString = getText(tree.get("maxString"))

    LearnedStringTransformation(minString, maxString)

  }

}
