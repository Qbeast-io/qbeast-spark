package io.qbeast.core.transform

import io.qbeast.core.model.QDataType
import io.qbeast.core.transform.StringHistogramTransformer.{defaultHist, defaultHistStr}

object StringHistogramTransformer extends TransformerType {
  override def transformerSimpleName: String = "string_hist"

  // "a" to "z"
  val defaultHist: Array[String] = (97 to 122).map(_.toChar.toString).toArray

  private val defaultHistStr: String = defaultHist.mkString("Array('", "', '", "')")
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
  override def stats: ColumnStats = ColumnStats(
    statsNames = colHist :: Nil,
    statsSqlPredicates = s"$defaultHistStr AS $colHist" :: Nil)

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row the values
   * @return the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    val hist = row(colHist) match {
      case h: Seq[_] => h.map(_.toString).toArray
      case _ => defaultHist
    }

    StringHistogramTransformation(hist)
  }

}
