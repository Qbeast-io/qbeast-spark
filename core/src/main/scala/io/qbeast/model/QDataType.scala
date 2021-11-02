package io.qbeast.model

trait QDataType {}

trait OrderedDataType extends QDataType {
  val ordering: Numeric[Any]
}
