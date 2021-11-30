/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.transform

import io.qbeast.model.{OrderedDataType, QDataType}

object LinearTransformer extends TransformerType {
  override def transformerSimpleName: String = "linear"

}

case class LinearTransformer(columnName: String, dataType: QDataType) extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  private def getValue(row: Any): Any = {
    row match {
      case d: java.math.BigDecimal => d.doubleValue()
      case other => other
    }
  }

  override def stats: ColumnStats =
    ColumnStats(
      Seq(colMax, colMin),
      Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin"))

  override def makeTransformation(row: String => Any): Transformation = {
    val min = getValue(row(colMin))
    val max = getValue(row(colMax))
    assert(min != null && max != null)
    dataType match {
      case ordered: OrderedDataType =>
        LinearTransformation(min, max, ordered)

    }
  }

  override protected def transformerType: TransformerType = LinearTransformer
}
