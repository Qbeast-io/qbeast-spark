/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    val qDataType = ColumnToIndexUtils.getColumnQType(columnName, schema)
    transformerType match {
      case Some(tt) => Transformer(tt, columnName, qDataType)
      case None => Transformer(columnName, qDataType)
    }
  }

}

object ColumnToIndex {

  /**
   * Creates an ColumnToIndex from a string which contains the column name and optionally the
   * Transformer type. e.g. "col_1" or "col_1:linear".
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
