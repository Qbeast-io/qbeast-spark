package io.qbeast.core.model

import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.types.StructType

/**
 * Represents a column to be indexed.
 * @param columnName
 *   The name of the column.
 * @param transformerType
 *   The type of the transformer, if available.
 */
case class ColumnToIndex(columnName: String, transformerType: Option[String]) {

  def toTransformer(schema: StructType): Transformer = {
    val getColumnQType = ColumnToIndexUtils.getColumnQType(columnName, schema)
    transformerType match {
      case Some(tt) => Transformer(tt, columnName, getColumnQType)
      case None => Transformer(columnName, getColumnQType)
    }
  }

}

/**
 * Companion object for IndexedColumn. Allows creating an IndexedColumn from a string.
 */

object ColumnToIndex {

  /**
   * Creates an IndexedColumn from a string and schema.
   * @param columnSpec
   * @return
   */
  def apply(columnSpec: String): ColumnToIndex = {
    columnSpec match {
      case ColumnToIndexUtils.SpecExtractor(columnName, transformerType) =>
        ColumnToIndex(columnName, Some(transformerType))
      case columnName =>
        ColumnToIndex(columnName, None)
    }
  }

}
