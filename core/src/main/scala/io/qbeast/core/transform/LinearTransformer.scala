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

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName the column name
 * @param dataType the data type of the column
 * @param optionalNullValue the optional null value
 */
case class LinearTransformer(
    columnName: String,
    dataType: QDataType,
    optionalNullValue: Option[Any] = None)
    extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  private def getValue(row: Any): Any = {
    row match {
      case d: java.math.BigDecimal => d.doubleValue()
      case other => other
    }
  }

  private def zeroValue = dataType match {
    case DoubleDataType => 0.0
    case IntegerDataType => 0
    case LongDataType => 0L
    case FloatDataType => 0.0f
    case DecimalDataType => 0.0
  }

  override def stats: ColumnStats =
    ColumnStats(
      statsNames = Seq(colMax, colMin),
      statsSqlPredicates = Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin"))

  override def makeTransformation(row: String => Any): Transformation = {
    val minAux = row(colMin)
    val maxAux = row(colMax)
    val (min, max) = if (minAux == null && maxAux == null) {
      // If all values are null we pick the same value for min and max
      val aux = getValue(optionalNullValue.getOrElse(zeroValue))
      (aux, aux)
    } else { // otherwhise we pick the min and max
      val min = getValue(minAux)
      val max = getValue(maxAux)
      (min, max)
    }
    dataType match {
      case ordered: OrderedDataType if optionalNullValue.isDefined =>
        LinearTransformation(min, max, getValue(optionalNullValue.get), ordered)
      case ordered: OrderedDataType =>
        LinearTransformation(min, max, ordered)
      case _ =>
        throw new IllegalArgumentException(
          s"LinearTransformer can only be used with OrderedDataType, not $dataType")

    }
  }

  override protected def transformerType: TransformerType = LinearTransformer
}
