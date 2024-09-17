package io.qbeast.core.transform

import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType

object CDFQuantilesTransformer extends TransformerType {
  override def transformerSimpleName: String = "quantiles"

  override def apply(columnName: String, dataType: QDataType): Transformer = {
    dataType match {
      case ord: OrderedDataType => CDFNumericQuantilesTransformer(columnName, ord)
      case StringDataType => CDFStringQuantilesTransformer(columnName)
      case _ =>
        throw new IllegalArgumentException(
          s"CDFQuantilesTransformer can only be applied to OrderedDataType columns or StringDataType columns. " +
            s"Column $columnName is of type $dataType")

    }
  }

}
