package io.qbeast.core.transform

import io.qbeast.core.model.{QDataType, StringDataType}

object HistogramTransformer extends TransformerType {
  override def transformerSimpleName: String = "histogram"

  override def apply(columnName: String, dataType: QDataType): Transformer = dataType match {
    case StringDataType => StringHistogramTransformer(columnName, dataType)
    case dt => throw new Exception(s"DataType not supported for HistogramTransformers: $dt")
  }

  // "a" to "z"
  def defaultStringHistogram: IndexedSeq[String] = (97 to 122).map(_.toChar.toString)
}

trait HistogramTransformer extends Transformer {

  override protected def transformerType: TransformerType = HistogramTransformer

  /**
   * Returns the name of the column
   *
   * @return
   */
  override def columnName: String

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row the values
   * @return the transformation
   */
  override def makeTransformation(row: String => Any): Transformation

}
