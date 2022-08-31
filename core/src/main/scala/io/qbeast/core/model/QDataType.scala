package io.qbeast.core.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonValue}

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

object TimestampDataType extends OrderedDataType {
  override def name: String = "TimestampDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]

}

object DateDataType extends OrderedDataType {
  override def name: String = "DateDataType"
  override val ordering: Numeric[Any] = implicitly[Numeric[Long]].asInstanceOf[Numeric[Any]]

}
