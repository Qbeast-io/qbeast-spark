/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.{
  DecimalDataType,
  DoubleDataType,
  FloatDataType,
  IntegerDataType,
  LongDataType,
  OrderedDataType,
  QDataType
}

import java.util.concurrent.ThreadLocalRandom

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName the column name
 * @param dataType the data type of the column
 */
case class LinearTransformer(
    columnName: String,
    override val dataType: QDataType,
    override val optionalNullValue: Option[Any])
    extends Transformer {

  private def getValue(value: Any): Any = {
    value match {
      case d: java.math.BigDecimal => d.doubleValue()
      case other => other
    }
  }

  private def generateRandomNumber(min: Any, max: Any): Any = {
    val random: ThreadLocalRandom = ThreadLocalRandom.current()
    dataType match {
      case DoubleDataType => random.nextDouble(min.asInstanceOf[Double], max.asInstanceOf[Double])
      case IntegerDataType => random.nextInt(min.asInstanceOf[Int], max.asInstanceOf[Int])
      case LongDataType => random.nextLong(min.asInstanceOf[Long], max.asInstanceOf[Long])
      case FloatDataType =>
        random.nextDouble(min.asInstanceOf[Float], max.asInstanceOf[Float]).toFloat
      case DecimalDataType =>
        random.nextDouble(min.asInstanceOf[Double], max.asInstanceOf[Double])
    }
  }

  override def makeTransformation(columnStats: ColumnStats): Transformation = {
    val min = getValue(columnStats.min)
    val max = getValue(columnStats.max)
    val nulls = optionalNullValue.getOrElse(generateRandomNumber(min, max))
    val nullValue = getValue(nulls)
    assert(min != null && max != null)
    dataType match {
      case ordered: OrderedDataType =>
        LinearTransformation(min, max, nullValue, ordered)

    }
  }

  override protected def transformerType: TransformerType = LinearTransformer
}
