package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

object StringHistogramTransformer extends TransformerType {
  override def transformerSimpleName: String = "string_hist"

}

case class StringHistogramTransformer(columnName: String, dataType: QDataType)
    extends Transformer {

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
    // TODO: To make a new Transformation, we are probably going to need the string histogram
    //  which requires access to a lot more information than just a single row.
    StringHistogramTransformation(Array.empty[String])
  }

}
