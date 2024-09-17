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

import io.qbeast.core.model.StringDataType

case class CDFStringQuantilesTransformer(columnName: String) extends CDFQuantilesTransformer {

  override val defaultQuantiles: IndexedSeq[String] = (97 to 122).map(_.toChar.toString)

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row
   *   the values
   * @return
   *   the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    val quantiles = row(columnQuantile) match {
      case h: Seq[_] => h.map(_.toString).toIndexedSeq
      case _ => defaultQuantiles
    }
    CDFQuantilesTransformation(quantiles, StringDataType)
  }

}
