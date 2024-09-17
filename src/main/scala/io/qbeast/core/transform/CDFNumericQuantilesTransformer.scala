package io.qbeast.core.transform

import io.qbeast.core.model.OrderedDataType

case class CDFNumericQuantilesTransformer(columnName: String, orderedDataType: OrderedDataType)
    extends Transformer {

  private val columnQuantiles = s"${columnName}_quantiles"

  /**
   * The default quantiles
   */
  private val defaultQuantiles: IndexedSeq[Any] = orderedDataType.defaultQuantiles

  override protected def transformerType: TransformerType = CDFQuantilesTransformer

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = {
    ColumnStats(columnQuantiles :: Nil, Nil)
  }

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row
   *   the values
   * @return
   *   the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {

    val quantiles = row(columnQuantiles) match {
      case h: Seq[_] => h.toIndexedSeq
      case _ => defaultQuantiles
    }
    CDFNumericQuantilesTransformation(quantiles, orderedDataType)

  }

}
