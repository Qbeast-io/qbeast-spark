package io.qbeast.spark.internal.expressions

import io.qbeast.core.model.IntegerDataType
import org.apache.spark.sql.connector.catalog.MetadataColumn
import org.apache.spark.sql.types.DataType

class QbeastWeightColumn extends MetadataColumn {
  override def name(): String = "qbeast.weight"

  override def dataType(): DataType = IntegerDataType
}
