/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.IISeq
import io.qbeast.core.model.{OrderedDataType, QDataType}

import java.sql.{Date, Timestamp}
import scala.collection.mutable

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
      // Very special case in which we load the transformation information from JSON options
      case d: java.lang.Long if dataType.name == "IntegerDataType" => d.intValue()
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

  private def getPercentiles(a: Any): IISeq[Any] = {
    a match {
      case arr: mutable.WrappedArray[_] => arr.toIndexedSeq.map(getValue)
      case _ => throw new Exception("No percentiles found")
    }
  }

  override def makeTransformation(row: String => Any): Transformation = {
    val minAux = row(colMin)
    val maxAux = row(colMax)
    val percentiles = getPercentiles(row(colPercentiles))
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
          LinearTransformation(min, max, percentiles, ordered)

      }
    }

  }

  override protected def transformerType: TransformerType = LinearTransformer
}
