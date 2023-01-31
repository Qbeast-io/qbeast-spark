/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.{OrderedDataType, QDataType}

import java.sql.{Date, Timestamp}

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName the column name
 * @param dataType the data type of the column
 */
case class LinearTransformer(columnName: String, dataType: QDataType) extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  private def getValue(row: Any): Any = {
    row match {
      case d: java.math.BigDecimal => d.doubleValue()
      case d: Timestamp => d.getTime()
      case d: Date => d.getTime()
      case other => other
    }
  }

  override def stats: ColumnStats =
    ColumnStats(
      statsNames = Seq(colMax, colMin),
      statsSqlPredicates =
        Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin", columnPercentiles))

  override def makeTransformation(row: String => Any): Transformation = {
    val minAux = row(colMin)
    val maxAux = row(colMax)
    if (minAux == null && maxAux == null) {
      // If all values are null,
      // we return a Transformation where null values are transformed to 0
      NullToZeroTransformation
    } else if (minAux == maxAux) {
      // If both values are equal we return an IdentityTransformation
      IdentityToZeroTransformation(minAux)
    } else { // otherwhise we pick the min and max
      val min = getValue(minAux)
      val max = getValue(maxAux)
      dataType match {
        case ordered: OrderedDataType =>
          LinearTransformation(min, max, ordered)

      }
    }

  }

  override protected def transformerType: TransformerType = LinearTransformer
}
