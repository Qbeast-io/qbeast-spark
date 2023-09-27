/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.{DoubleNode, IntNode, NumericNode, TextNode}
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import io.qbeast.core.model.{
  DateDataType,
  DecimalDataType,
  DoubleDataType,
  FloatDataType,
  IntegerDataType,
  LongDataType,
  OrderedDataType,
  TimestampDataType
}

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.time.Instant
import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
 * A linear transformation of a coordinate based on min max values
 * @param minNumber minimum value of the space
 * @param maxNumber maximum value of the space
 * @param nullValue the value to use for null coordinates
 * @param orderedDataType ordered data type of the coordinate
 */
@JsonSerialize(using = classOf[LinearTransformationSerializer])
@JsonDeserialize(using = classOf[LinearTransformationDeserializer])
case class LinearTransformation(
    minNumber: Any,
    maxNumber: Any,
    nullValue: Any,
    orderedDataType: OrderedDataType)
    extends Transformation {

  import orderedDataType.ordering._

  private val mn = minNumber.toDouble

  private val scale: Double = {
    val mx = maxNumber.toDouble
    require(mx > mn, "Range cannot be not null, and max must be > min")
    1.0 / (mx - mn)
  }

  override def transform(value: Any): Double = {
    val v = if (value == null) nullValue else value
    v match {
      case v: Double => (v - mn) * scale
      case v: Long => (v - mn) * scale
      case v: Int => (v - mn) * scale
      case v: BigDecimal => (v.doubleValue() - mn) * scale
      case v: Float => (v - mn) * scale
      case v: Timestamp => (v.getTime - mn) * scale
      case v: Date => (v.getTime - mn) * scale
      case v: Instant => (v.toEpochMilli - mn) * scale
    }
  }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   * and the other transformation. The range of the resulting transformation
   * is the intersection of this and the other transformation,
   * which can be a LinearTransformation or IdentityTransformation
   * @param other
   * @return a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = {
    other match {
      case LinearTransformation(otherMin, otherMax, otherNullValue, otherOrdering)
          if orderedDataType == otherOrdering =>
        LinearTransformation(
          min(minNumber, otherMin),
          max(maxNumber, otherMax),
          otherNullValue,
          orderedDataType)
          .asInstanceOf[Transformation]
      case IdentityToZeroTransformation(newVal) =>
        val otherNullValue =
          LinearTransformationUtils.generateRandomNumber(
            min(minNumber, newVal),
            max(maxNumber, newVal),
            Option(42.toLong))
        val orderedDataType = this.orderedDataType
        LinearTransformation(
          min(minNumber, newVal),
          max(maxNumber, newVal),
          otherNullValue,
          orderedDataType)
          .asInstanceOf[Transformation]

    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   * @param newTransformation the new transformation created with statistics over the new data
   * @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case LinearTransformation(newMin, newMax, _, otherOrdering)
          if orderedDataType == otherOrdering =>
        gt(minNumber, newMin) || lt(maxNumber, newMax)
      case IdentityToZeroTransformation(newVal) =>
        gt(minNumber, newVal) || lt(maxNumber, newVal)
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
    writeNumb(gen, "nullValue", value.nullValue)
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
    writeNumb(gen, "nullValue", value.nullValue)
    gen.writeObjectField("orderedDataType", value.orderedDataType)
    gen.writeEndObject()
  }

}

object LinearTransformation {

  /**
   * Creates a LinearTransformation that has random value for the nulls
   * within the [minNumber, maxNumber] range
   * @param minNumber
   * @param maxNumber
   * @param orderedDataType
   * @param seed
   * @return
   */

  def apply(
      minNumber: Any,
      maxNumber: Any,
      orderedDataType: OrderedDataType,
      seed: Option[Long] = None): LinearTransformation = {
    val randomNull = LinearTransformationUtils.generateRandomNumber(minNumber, maxNumber, seed)
    LinearTransformation(minNumber, maxNumber, randomNull, orderedDataType)
  }

}

class LinearTransformationDeserializer
    extends StdDeserializer[LinearTransformation](classOf[LinearTransformation]) {

  private def getTypedValue(odt: OrderedDataType, tree: TreeNode): Any = {
    (odt, tree) match {
      case (IntegerDataType, int: IntNode) => int.asInt
      case (DoubleDataType, double: DoubleNode) => double.asDouble
      case (LongDataType, long: NumericNode) => long.asLong
      case (FloatDataType, float: DoubleNode) => float.floatValue
      case (DecimalDataType, decimal: DoubleNode) => decimal.asDouble
      case (TimestampDataType, timestamp: NumericNode) => timestamp.asLong
      case (DateDataType, date: NumericNode) => date.asLong
      case (_, null) => null
      case (a, b) =>
        throw new IllegalArgumentException(s"Invalid data type  ($a,$b) ${b.getClass} ")

    }

  }

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): LinearTransformation = {
    val json = p.getCodec
    val tree: TreeNode = json
      .readTree(p)

    val odt = tree.get("orderedDataType") match {
      case tn: TextNode => OrderedDataType(tn.asText())
    }

    val min = getTypedValue(odt, tree.get("minNumber"))
    val max = getTypedValue(odt, tree.get("maxNumber"))
    val nullValue = getTypedValue(odt, tree.get("nullValue"))

    if (nullValue == null) {
      // the hash acts like a seed to generate the same random null value
      val hash = MurmurHash3.stringHash(tree.toString)
      LinearTransformation(min, max, odt, seed = Some(hash))
    } else LinearTransformation(min, max, nullValue, odt)

  }

}

object LinearTransformationUtils {

  /**
   * Creates a LinearTransformationUtils object that contains
   * useful functions that can be used outside of the LinearTransformation class.
   * @param minNumber
   * @param maxNumber
   * @param orderedDataType
   * @param seed
   * @return
   */

  def generateRandomNumber(min: Any, max: Any, seed: Option[Long]): Any = {
    val r = if (seed.isDefined) new Random(seed.get) else new Random()
    val random = r.nextDouble()

    (min, max) match {
      case (min: Double, max: Double) => min + (random * (max - min))
      case (min: Long, max: Long) => min + (random * (max - min)).toLong
      case (min: Int, max: Int) => min + (random * (max - min)).toInt
      case (min: Float, max: Float) => min + (random * (max - min)).toFloat
      case (min, max) =>
        throw new IllegalArgumentException(
          s"Cannot generate random number for type ${min.getClass.getName}")

    }
  }

}
