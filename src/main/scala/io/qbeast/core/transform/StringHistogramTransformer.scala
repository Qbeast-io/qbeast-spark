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
package io.qbeast.core.transform

import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType
import io.qbeast.core.transform.StringHistogramTransformer.defaultStringHistogram

object StringHistogramTransformer extends TransformerType {
  override def transformerSimpleName: String = "histogram"

  override def apply(columnName: String, dataType: QDataType): Transformer = {
    if (dataType != StringDataType) {
      throw new IllegalArgumentException(
        s"StringHistogramTransformer can only be applied to String columns. " +
          s"Column $columnName is of type $dataType")
    } else {
      StringHistogramTransformer(columnName)
    }
  }

  val defaultStringHistogram: IndexedSeq[String] = (97 to 122).map(_.toChar.toString)

}

case class StringHistogramTransformer(columnName: String) extends Transformer {
  private val columnHistogram = s"${columnName}_histogram"

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = {
    val defaultHistString = defaultStringHistogram.mkString("Array('", "', '", "')")
    ColumnStats(
      statsNames = columnHistogram :: Nil,
      statsSqlPredicates = s"$defaultHistString AS $columnHistogram" :: Nil)
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
    val hist = row(columnHistogram) match {
      case h: Seq[_] => h.map(_.toString).toIndexedSeq
      case _ => defaultStringHistogram
    }

    StringHistogramTransformation(hist)
  }

  override protected def transformerType: TransformerType = StringHistogramTransformer
}
