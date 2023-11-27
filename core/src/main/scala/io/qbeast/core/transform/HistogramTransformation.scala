package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

trait HistogramTransformation extends Transformation {

  /**
   * QDataType for the associated column.
   */
  def dataType: QDataType

  /**
   * Histogram of the associated column that reflects the distribution of the column values.
   * @return
   */
  def histogram: IndexedSeq[Any]

  /**
   * Determines whether the associated histogram is the default one
   * @return
   */
  def isDefault: Boolean

  override def transform(value: Any): Double

  override def isSupersededBy(newTransformation: Transformation): Boolean

  override def merge(other: Transformation): Transformation
}
