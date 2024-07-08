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
package io.qbeast.core.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonValue

/**
 * Companion object for QDataType
 */
object QDataType {

  val qtypes: Map[String, QDataType] =
    Map(StringDataType.name -> StringDataType) ++ OrderedDataType.qtypes // TODO add more types

  @JsonCreator
  def apply(name: String): QDataType = qtypes(name)

}

/**
 * Data type for a Qbeast column
 */
trait QDataType extends Serializable {

  @JsonValue
  def name: String

}

/**
 * Companion object for OrderedDataType
 */
object OrderedDataType {

  val qtypes: Map[String, OrderedDataType] =
    Seq(
      DoubleDataType,
      IntegerDataType,
      FloatDataType,
      LongDataType,
      DecimalDataType,
      TimestampDataType,
      DateDataType)
      .map(dt => dt.name -> dt)
      .toMap

  @JsonCreator
  def apply(name: String): OrderedDataType = qtypes(name)

}

/**
 * Data type with ordering properties
 */
trait OrderedDataType extends QDataType {

  @JsonIgnore
  val ordering: Numeric[Any]

  val defaultHistogram: IndexedSeq[Any]

}

object DoubleDataType extends OrderedDataType {
  override def name: String = "DoubleDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]

  override val defaultHistogram: IndexedSeq[Any] =
    BigDecimal(Double.MinValue).to(BigDecimal(Double.MaxValue), BigDecimal(1e307)).map(_.toDouble)

}

object IntegerDataType extends OrderedDataType {
  override def name: String = "IntegerDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]
  override val defaultHistogram: IndexedSeq[Any] = Int.MinValue.to(Int.MaxValue)
}

object LongDataType extends OrderedDataType {
  override def name: String = "LongDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]
  override val defaultHistogram: IndexedSeq[Any] = Long.MinValue.to(Long.MaxValue)
}

object FloatDataType extends OrderedDataType {
  override def name: String = "FloatDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Float]].asInstanceOf[Numeric[Any]]

  override val defaultHistogram: IndexedSeq[Any] =
    BigDecimal(Float.MinValue.toDouble)
      .to(BigDecimal(Float.MaxValue.toDouble), BigDecimal(1e37))
      .map(_.toFloat)

}

object DecimalDataType extends OrderedDataType {
  override def name: String = "DecimalDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]

  override val defaultHistogram: IndexedSeq[Any] =
    BigDecimal(Double.MinValue).to(BigDecimal(Double.MaxValue), BigDecimal(1e307)).map(_.toDouble)

}

object StringDataType extends QDataType {
  override def name: String = "StringDataType"
}

object TimestampDataType extends OrderedDataType {
  override def name: String = "TimestampDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]
  override val defaultHistogram: IndexedSeq[Any] = Long.MinValue.to(Long.MaxValue)

}

object DateDataType extends OrderedDataType {
  override def name: String = "DateDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]
  override val defaultHistogram: IndexedSeq[Any] = Long.MinValue.to(Long.MaxValue)

}
