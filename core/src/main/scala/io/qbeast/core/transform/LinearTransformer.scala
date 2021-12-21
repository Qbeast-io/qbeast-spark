/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.{OrderedDataType, QDataType}

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName the column name
 * @param dataType the data type of the column
 */
case class LinearTransformer(columnName: String, override val dataType: QDataType)
    extends Transformer {

  private def getValue(value: Any): Any = {
    value match {
      case d: java.math.BigDecimal => d.doubleValue()
      case other => other
    }
  }

  override def makeTransformation(columnStats: ColumnStats): Transformation = {
    val min = getValue(columnStats.min)
    val max = getValue(columnStats.max)
    assert(min != null && max != null)
    dataType match {
      case ordered: OrderedDataType =>
        LinearTransformation(min, max, ordered)

    }
  }

  override protected def transformerType: TransformerType = LinearTransformer
}
