package io.qbeast.core.transform

import io.qbeast.core.model.QDataType
import io.qbeast.core.transform.HistogramTransformer.defaultStringHistogram

case class StringHistogramTransformer(columnName: String, dataType: QDataType)
    extends HistogramTransformer {
  private val columnHistogram = s"${columnName}_histogram"

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = {
    val defaultHistString = defaultStringHistogram.mkString("Array('", "', '", "')")
    ColumnStats(
      statsNames = columnHistogram :: Nil,
      statsSqlPredicates = s"$defaultHistString AS $columnHistogram" :: Nil)
  }

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row the values
   * @return the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    val hist = row(columnHistogram) match {
      case h: Seq[_] => h.map(_.toString).toIndexedSeq
      case _ => defaultStringHistogram
    }

    StringHistogramTransformation(hist)
  }

}
