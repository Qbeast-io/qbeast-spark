package io.qbeast.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonValue}

object QDataType {
  val qtypes: Map[String, OrderedDataType] = OrderedDataType.qtypes // TODO add more types

  @JsonCreator
  def apply(name: String): QDataType = qtypes(name)

}

trait QDataType extends Serializable {

  @JsonValue
  def name: String

}

object OrderedDataType {

  val qtypes = Map(
    "DoubleDataType" -> DoubleDataType,
    "IntegerDataType" -> IntegerDataType,
    "FloatDataType" -> FloatDataType,
    "LongDataType" -> LongDataType)

  @JsonCreator
  def apply(name: String): OrderedDataType = qtypes(name)

}

trait OrderedDataType extends QDataType {

  @JsonIgnore
  val ordering: Numeric[Any]

}

class DoubleDataType extends OrderedDataType {

  @JsonValue
  override def name: String = "DoubleDataType"

  override val ordering: Numeric[Any] = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
}

object DoubleDataType extends DoubleDataType

object IntegerDataType extends OrderedDataType {
  override def name: String = "IntegerDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]
}

object LongDataType extends OrderedDataType {
  override def name: String = "LongDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]
}

object FloatDataType extends OrderedDataType {
  override def name: String = "FloatDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Float]].asInstanceOf[Numeric[Any]]
}
