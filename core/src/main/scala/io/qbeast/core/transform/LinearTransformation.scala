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
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import io.qbeast.core.model.DateDataType
import io.qbeast.core.model.DecimalDataType
import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.FloatDataType
import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.TimestampDataType

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
import scala.util.hashing.MurmurHash3
import scala.util.Random

/**
 * A linear transformation of a coordinate based on min max values
 * @param minNumber
 *   minimum value of the space
 * @param maxNumber
 *   maximum value of the space
 * @param nullValue
 *   the value to use for null coordinates
 * @param orderedDataType
 *   ordered data type of the coordinate
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
   * and the other transformation.
   */
  override def merge(other: Transformation): Transformation = {
    other match {
      case LinearTransformation(otherMin, otherMax, _, otherOrdering)
          if orderedDataType == otherOrdering =>
        LinearTransformation(min(minNumber, otherMin), max(maxNumber, otherMax), orderedDataType)
      case IdentityTransformation(newVal, otherType) if orderedDataType == otherType =>
        LinearTransformation(min(minNumber, newVal), max(maxNumber, newVal), orderedDataType)
      case IdentityToZeroTransformation(newVal) =>
        LinearTransformation(min(minNumber, newVal), max(maxNumber, newVal), orderedDataType)
      case _ => this
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   * @param newTransformation
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case LinearTransformation(newMin, newMax, _, otherOrdering)
          if orderedDataType == otherOrdering =>
        gt(minNumber, newMin) || lt(maxNumber, newMax)
      case IdentityTransformation(newVal, otherType)
          if otherType == orderedDataType && newVal != null =>
        gt(minNumber, newVal) || lt(maxNumber, newVal)
      case IdentityToZeroTransformation(newVal) => gt(minNumber, newVal) || lt(maxNumber, newVal)
      case _ => false
    }

}

object LinearTransformation {

  /**
   * Creates a LinearTransformation that has random value for the nulls within the [minNumber,
   * maxNumber] range
   * @param minNumber
   *   the minimum value of the transformation
   * @param maxNumber
   *   the maximum value of the transformation
   * @param orderedDataType
   *   the ordered data type of the transformation
   * @param seed
   *   the seed to generate the random null value
   * @return
   */
  def apply(
      minNumber: Any,
      maxNumber: Any,
      orderedDataType: OrderedDataType,
      seed: Option[Long] = None): LinearTransformation = {
    val randomNull = generateRandomNumber(minNumber, maxNumber, seed)
    LinearTransformation(minNumber, maxNumber, randomNull, orderedDataType)
  }

  /**
   * Creates a LinearTransformationUtils object that contains useful functions that can be used
   * outside the LinearTransformation class.
   */
  private[transform] def generateRandomNumber(min: Any, max: Any, seed: Option[Long]): Any = {
    val r = if (seed.isDefined) new Random(seed.get) else new Random()
    val random = r.nextDouble()
    (min, max) match {
      case (min: Double, max: Double) => min + (random * (max - min))
      case (min: Long, max: Long) => min + (random * (max - min)).toLong
      case (min: Int, max: Int) => min + (random * (max - min)).toInt
      case (min: Float, max: Float) => min + (random * (max - min)).toFloat
      case _ =>
        throw new IllegalArgumentException("Cannot generate random number for " +
          s"(min:type, max:type) = ($min: ${min.getClass.getName}, $max: ${max.getClass.getName})")
    }
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
