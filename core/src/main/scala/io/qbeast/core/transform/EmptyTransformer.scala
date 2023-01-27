package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

object EmptyTransformer extends TransformerType {
  override def transformerSimpleName: String = "empty"

  override def apply(columnName: String, dataType: QDataType): Transformer =
    EmptyTransformer(columnName)

}

/**
 * An empty Transformer meant for empty revisions
 */
case class EmptyTransformer(columnName: String) extends Transformer {
  override protected def transformerType: TransformerType = EmptyTransformer

  override def stats: ColumnStats = NoColumnStats

  override def makeTransformation(row: String => Any): Transformation = EmptyTransformation()
}
