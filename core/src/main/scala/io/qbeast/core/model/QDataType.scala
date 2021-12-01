package io.qbeast.core.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonValue}

object QDataType {

  val qtypes: Map[String, QDataType] =
    Map(StringDataType.name -> StringDataType) ++ OrderedDataType.qtypes // TODO add more types

  @JsonCreator
  def apply(name: String): QDataType = qtypes(name)

}

trait QDataType extends Serializable {

  @JsonValue
  def name: String

}

object OrderedDataType {

  val qtypes = Seq(DoubleDataType, IntegerDataType, FloatDataType, LongDataType, DecimalDataType)
    .map(dt => dt.name -> dt)
    .toMap

  @JsonCreator
  def apply(name: String): OrderedDataType = qtypes(name)

}

trait OrderedDataType extends QDataType {

  @JsonIgnore
  val ordering: Numeric[Any]

}

object DoubleDataType extends OrderedDataType {
  override def name: String = "DoubleDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
}

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

object DecimalDataType extends OrderedDataType {
  override def name: String = "DecimalDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
}

object StringDataType extends QDataType {
  override def name: String = "StringDataType"
}
