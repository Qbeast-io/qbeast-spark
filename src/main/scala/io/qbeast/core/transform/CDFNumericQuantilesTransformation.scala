package io.qbeast.core.transform

import io.qbeast.core.model.OrderedDataType

case class CDFNumericQuantilesTransformation(
    quantiles: IndexedSeq[Any],
    orderedDataType: OrderedDataType)
    extends CDFQuantilesTransformation {

  override implicit val ordering: Ordering[Any] = orderedDataType.ordering

  override def mapValue(value: Any): Any = value

}
