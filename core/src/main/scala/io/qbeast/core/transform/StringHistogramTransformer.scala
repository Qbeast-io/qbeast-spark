package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

object StringHistogramTransformer extends TransformerType {
  override def transformerSimpleName: String = "string_hist"

}

case class StringHistogramTransformer(columnName: String, dataType: QDataType)
    extends Transformer {
  private val colHist = s"${columnName}_hist"

  override protected def transformerType: TransformerType = StringHistogramTransformer

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = NoColumnStats

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row the values
   * @return the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    val hist = row(colHist) match {
      case h: Seq[_] => h.map(_.toString).toArray
      case _ => Array.empty[String]
    }
    StringHistogramTransformation(hist)
  }

}
