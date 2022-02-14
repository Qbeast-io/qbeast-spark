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

import scala.util.Random

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName the column name
 * @param dataType the data type of the column
 */
case class LinearTransformer(columnName: String, dataType: QDataType, nullValue: Any = null)
    extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  private def getValue(row: Any): Any = {
    row match {
      case d: java.math.BigDecimal => d.doubleValue()
      case other => other
    }
  }

  private def generateRandomNumber(min: Any, max: Any): Any = {
    val random = Random.nextDouble()

    dataType match {
      case DoubleDataType =>
        min
          .asInstanceOf[Double] + (random * (max.asInstanceOf[Double] - min.asInstanceOf[Double]))
      case IntegerDataType =>
        min.asInstanceOf[Int] + (random * (max.asInstanceOf[Int] - min.asInstanceOf[Int])).toInt
      case LongDataType =>
        min.asInstanceOf[Long] + (random * (max.asInstanceOf[Long] - min
          .asInstanceOf[Long])).toLong
      case FloatDataType =>
        min.asInstanceOf[Float] + (random * (max.asInstanceOf[Float] - min
          .asInstanceOf[Float])).toFloat
      case DecimalDataType =>
        min
          .asInstanceOf[Double] + (random * (max.asInstanceOf[Double] - min.asInstanceOf[Double]))
    }
  }

  private lazy val (minValue, maxValue): (Any, Any) = {
    dataType match {
      case DoubleDataType => (Double.MinValue, Double.MaxValue)
      case IntegerDataType => (Int.MinValue, Int.MaxValue)
      case LongDataType => (Long.MinValue, Long.MaxValue)
      case FloatDataType => (Float.MinValue, Float.MaxValue)
      case DecimalDataType => (Double.MinValue, Double.MaxValue)
    }
  }

  override def stats: ColumnStats =
    ColumnStats(
      statsNames = Seq(colMax, colMin),
      statsSqlPredicates = Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin"))

  override def makeTransformation(row: String => Any): Transformation = {
    val minAux = row(colMin)
    val maxAux = row(colMax)
    val (min, max) = {
      // TODO If all values are null...
      if (minAux == null && maxAux == null) {
        (minValue, maxValue)
      } else {
        val min = getValue(row(colMin))
        val max = getValue(row(colMax))
        (min, max)
      }
    }

    val n = if (nullValue == null) {
      getValue(generateRandomNumber(min, max))
    } else getValue(nullValue)

    dataType match {
      case ordered: OrderedDataType =>
        LinearTransformation(min, max, n, ordered)

    }
  }

  override protected def transformerType: TransformerType = LinearTransformer
}
