/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.QDataType

import java.sql.Date
import java.sql.Timestamp
import java.time.Instant

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

/**
 * Linear Transformer specification of a column
 * @param columnName
 *   the column name
 * @param dataType
 *   the data type of the column
 */
case class LinearTransformer(columnName: String, dataType: QDataType) extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  private def getValue(row: Any): Any = {
    row match {
      // Very special case in which we load the transformation information from JSON options
      case d: java.lang.Long if dataType.name == "IntegerDataType" => d.intValue()
      case d: java.math.BigDecimal => d.doubleValue()
      case d: Timestamp => d.getTime
      case d: Date => d.getTime
      case d: Instant => d.toEpochMilli
      case other => other
    }
  }

  override def stats: ColumnStats =
    ColumnStats(
      statsNames = Seq(colMax, colMin),
      statsSqlPredicates = Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin"))

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
    } else { // otherwise we pick the min and max
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
