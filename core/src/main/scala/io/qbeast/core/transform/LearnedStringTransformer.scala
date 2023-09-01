/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

object LearnedStringTransformer extends TransformerType {
  override def transformerSimpleName: String = "learned"
}

case class LearnedStringTransformer(columnName: String, dataType: QDataType) extends Transformer {
  private def colMax = s"${columnName}_max"
  private def colMin = s"${columnName}_min"

  override protected def transformerType: TransformerType = LearnedStringTransformer

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = ColumnStats(
    statsNames = Seq(colMax, colMin),
    statsSqlPredicates = Seq(s"max($columnName) AS $colMax", s"min($columnName) AS $colMin"))

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row the values
   * @return the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    val min = row(colMin)
    val max = row(colMax)
    // If all values are null, we map nulls to 0
    // If both values are equal we return an IdentityTransformation
    // otherwise we the transformation using the min and max string
    if (min == null && max == null) NullToZeroTransformation
    else if (min == max) IdentityToZeroTransformation(min)
    else LearnedStringTransformation(min.toString, max.toString)
  }

}
